from __future__ import absolute_import
import datetime
import os, time
import logging
from airbrake.notifier import Airbrake
from sqlalchemy.exc import IntegrityError

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
from sqlalchemy import Column, String, Integer, PickleType
from sqlalchemy import UniqueConstraint
from sqlalchemy import or_, and_

from frontera import Backend
from frontera.core.models import Response as frontera_response

# Default settings
from frontera.utils.misc import load_object
from frontera.utils.url import canonicalize_url

DEFAULT_ENGINE = 'sqlite:///:memory:'
DEFAULT_ENGINE_ECHO = False
DEFAULT_DROP_ALL_TABLES = False
DEFAULT_CLEAR_CONTENT = False
Base = declarative_base()

DEBUG = False if os.environ.get("env", 'DEBUG') == 'PRODUCTION' else True


class DatetimeTimestamp(TypeDecorator):

    impl = String  # To use milliseconds in mysql
    timestamp_format = '%Y%m%d%H%M%S%f'

    def process_bind_param(self, value, _):
        if isinstance(value, datetime.datetime):
            return value.strftime(self.timestamp_format)
        raise ValueError('Not valid datetime')

    def process_result_value(self, value, _):
        return datetime.datetime.strptime(value, self.timestamp_format)


class PageMixin(object):
    __table_args__ = (
        UniqueConstraint('url'),
        {
            'mysql_charset': 'utf8',
            'mysql_engine': 'InnoDB',
            'mysql_row_format': 'DYNAMIC',
        },
    )

    class State:
        NOT_CRAWLED = 'NOT CRAWLED'
        QUEUED = 'QUEUED'
        CRAWLED = 'CRAWLED'
        ERROR = 'ERROR'

    url = Column(String(1024), nullable=False)
    fingerprint = Column(String(40), primary_key=True, nullable=False, index=True, unique=True)
    depth = Column(Integer, nullable=False)
    created_at = Column(DatetimeTimestamp(20), nullable=False)
    status_code = Column(String(20))
    state = Column(String(12), index=True)
    error = Column(String(50))
    meta = Column(PickleType())
    headers = Column(PickleType())
    cookies = Column(PickleType())
    method = Column(String(6))
    body = Column(PickleType())
    retries = Column(Integer(), default=0)

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<Page:%s>' % self.url


class SQLiteBackend(Backend):
    component_name = 'SQLite Backend'
    spider_args = []
    spider_kwargs = {}

    def __init__(self, manager):
        self.manager = manager
        self.pages_crawled_in_current_batch = 0
        # Get settings
        settings = manager.settings
        self.frontier = settings.attributes.get('spider_settings', {}).get('frontier')

        assert 'frontier' in settings.attributes.get('spider_settings', {}), "frontier missing in frontera settings"

        class Page(PageMixin, Base):
            __tablename__ = self.frontier

        self.page_model = Page

        self.spider_args = settings.attributes.get('spider_settings', {}).get('args', [])
        self.spider_kwargs = settings.attributes.get('spider_settings', {}).get('kwargs', {})
        self.airbrake = Airbrake(api_key=self.spider_kwargs['AIRBRAKE_API_KEY'], project_id=self.spider_kwargs['AIRBRAKE_PROJECT_ID'])
        self.retry_times = self.spider_kwargs.get('RETRY_TIMES', 0)
        self.retry_http_codes = self.spider_kwargs.get('RETRY_HTTP_CODES', [])
        self.exceptions_to_retry = self.spider_kwargs.get('EXCEPTIONS_TO_RETRY', [])

        self.new_scrape = self.spider_kwargs.get('new_scrape', False)

        # Get settings
        engine = settings.get('SQLALCHEMYBACKEND_ENGINE', DEFAULT_ENGINE)
        engine_echo = settings.get('SQLALCHEMYBACKEND_ENGINE_ECHO', DEFAULT_ENGINE_ECHO)
        drop_all_tables = settings.get('SQLALCHEMYBACKEND_DROP_ALL_TABLES', DEFAULT_DROP_ALL_TABLES)
        clear_content = settings.get('SQLALCHEMYBACKEND_CLEAR_CONTENT', DEFAULT_CLEAR_CONTENT)

        # Create engine
        self.engine = create_engine(engine, echo=engine_echo)

        if self.new_scrape:
            connection = self.engine.connect()
            connection.execute('DROP INDEX IF EXISTS ix_{}_fingerprint;'.format(self.frontier))
            connection.execute('DROP INDEX IF EXISTS ix_{}_state;'.format(self.frontier))
            connection.execute('ALTER TABLE IF EXISTS {} RENAME TO {}_{};'.format(self.frontier, self.frontier, int(time.time())))
            connection.close()

        # Drop tables if we have to
        if drop_all_tables:
            Base.metadata.drop_all(self.engine)

        Base.metadata.create_all(self.engine)

        # Create session
        self.Session = sessionmaker()
        self.Session.configure(bind=self.engine)
        self.session = self.Session()

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.session.commit()
        self.session.close()
        self.engine.dispose()

    def log(self, message, errtype=None, extra={}):
        if not DEBUG:
            self.airbrake.environment = self.frontier
            self.airbrake.log(message, errtype=errtype, extra=extra)
        else:
            self.manager.logger.backend.debug('{}:{}'.format(errtype, message))
            logging.exception('{}:{}'.format(errtype, message))

    def add_seeds(self, seeds):
        for seed in seeds:
            db_page, _ = self._get_or_create_db_page(seed)
        self.session.commit()

    def get_next_requests(self, max_next_requests, **kwargs):
        query = self.page_model.query(self.session).with_lockmode('update')
        query = query.filter(
            or_(
                and_(self.page_model.state == PageMixin.State.ERROR, self.page_model.error.in_(self.exceptions_to_retry), self.page_model.retries < self.retry_times).self_group(),
                and_(self.page_model.state == PageMixin.State.ERROR, self.page_model.status_code.in_(self.retry_http_codes), self.page_model.retries < self.retry_times).self_group(),
                and_(self.page_model.state == PageMixin.State.NOT_CRAWLED, self.page_model.retries < self.retry_times).self_group()
            ))

        query = self._get_order_by(query)
        if max_next_requests:
            query = query.limit(max_next_requests)
        next_pages = []
        for db_page in query:
            db_page.state = PageMixin.State.QUEUED
            request = self.manager.request_model(url=db_page.url, meta=db_page.meta,
                                                 headers=db_page.headers, cookies=db_page
                                                 .cookies, method=db_page.method, body=db_page.body)
            next_pages.append(request)

        self.session.commit()
        return next_pages

    def page_crawled(self, response, links):
        db_page, created = self._get_or_create_db_page(response)
        db_page.state = PageMixin.State.CRAWLED
        db_page.status_code = response.status_code

        redirected_urls = db_page.meta.get('scrapy_meta', {}).get('redirect_urls', [])
        for url in redirected_urls:
            self.fingerprint_function = load_object(self.manager.settings.get('URL_FINGERPRINT_FUNCTION'))
            fingerprint = self.fingerprint_function(canonicalize_url(url))
            redirected_page = self.page_model.query(self.session).filter_by(fingerprint=fingerprint).first()
            if redirected_page:
                redirected_page.state = self.page_model.State.CRAWLED

        for link in links:
            db_page_from_link, created = self._get_or_create_db_page(link)
            if created:
                db_page_from_link.depth = db_page.depth+1
        self.session.commit()

    def request_error(self, request, error):
        db_page, _ = self._get_or_create_db_page(request)
        db_page.state = PageMixin.State.ERROR
        status = request.meta.get('scrapy_meta', {}).get('error_status', None)
        if status:
            db_page.status_code = status
        db_page.error = error
        db_page.retries += 1
        db_page.meta = request.meta

        if db_page.retries >= self.retry_times:
            self.log(db_page.status_code, errtype=error)

        self.session.commit()

    def _create_page(self, obj):
        db_page = self.page_model()
        db_page.fingerprint = obj.meta['fingerprint']
        db_page.state = PageMixin.State.NOT_CRAWLED
        db_page.url = obj.url
        db_page.created_at = datetime.datetime.utcnow()
        db_page.meta = obj.meta
        db_page.depth = 0

        if not isinstance(obj, frontera_response):
            db_page.headers = obj.headers
            db_page.method = obj.method
            db_page.cookies = obj.cookies
            if obj.method.lower() == 'post':
                db_page.body = obj.body

        return db_page

    def _get_or_create_db_page(self, obj):
        if not self._request_exists(obj.meta['fingerprint']):
            db_page = self._create_page(obj)
            try:
                self.session.add(db_page)
                self.session.commit()
                self.manager.logger.backend.debug('Creating request %s' % db_page)
            except IntegrityError as e:
                self.session.rollback()
                reason = e.message
                if "Duplicate entry" in reason:
                    self.manager.logger.backend.debug("Trying to write duplicate entry for url {}".format(obj.url))
                else:
                    self.log(e.message)
                    raise e
            return db_page, True
        else:
            db_page = self.page_model.query(self.session)\
                .filter_by(fingerprint=obj.meta['fingerprint']).first()
            self.manager.logger.backend.debug('Request exists %s' % db_page)
            return db_page, False

    def _request_exists(self, fingerprint):
        q = self.page_model.query(self.session).filter_by(fingerprint=fingerprint)
        return self.session.query(q.exists()).scalar()

    def _get_order_by(self, query):
        raise NotImplementedError


class FIFOBackend(SQLiteBackend):
    component_name = 'SQLite FIFO Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.created_at)


class LIFOBackend(SQLiteBackend):
    component_name = 'SQLite LIFO Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.created_at.desc())


class DFSBackend(SQLiteBackend):
    component_name = 'SQLite DFS Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.depth.desc(), self.page_model.created_at)


class BFSBackend(SQLiteBackend):
    component_name = 'SQLite BFS Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.depth, self.page_model.created_at)


BASE = SQLiteBackend
LIFO = LIFOBackend
FIFO = FIFOBackend
DFS = DFSBackend
BFS = BFSBackend