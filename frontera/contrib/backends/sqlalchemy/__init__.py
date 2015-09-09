from __future__ import absolute_import
import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
from sqlalchemy import Column, String, Integer, PickleType
from sqlalchemy import UniqueConstraint

from frontera import Backend
from frontera.core.models import Response as frontera_response

# Default settings
DEFAULT_ENGINE = 'sqlite:///:memory:'
DEFAULT_ENGINE_ECHO = False
DEFAULT_DROP_ALL_TABLES = False
DEFAULT_CLEAR_CONTENT = False
UPDATE_STATUS_AFTER = 1000
Base = declarative_base()

DEBUG = True

if DEBUG:
    import logging
    logger = logging.getLogger('crawler_logger')
else:
    import airbrake
    logger = airbrake.getLogger(api_key='8361ef91f26e6e6a5187c8820c339f67', project_id=115420)

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

        assert 'frontier' in settings.attributes.get('spider_settings', {}), "frontier missing in frontera settings"

        class Page(PageMixin, Base):
            __tablename__ = settings.attributes.get('spider_settings', {}).get('frontier')

        self.page_model = Page

        self.spider_args = settings.attributes.get('spider_settings', {}).get('args', [])
        self.spider_kwargs = settings.attributes.get('spider_settings', {}).get('kwargs', {})

        # Get settings
        engine = settings.get('SQLALCHEMYBACKEND_ENGINE', DEFAULT_ENGINE)
        engine_echo = settings.get('SQLALCHEMYBACKEND_ENGINE_ECHO', DEFAULT_ENGINE_ECHO)
        drop_all_tables = settings.get('SQLALCHEMYBACKEND_DROP_ALL_TABLES', DEFAULT_DROP_ALL_TABLES)
        clear_content = settings.get('SQLALCHEMYBACKEND_CLEAR_CONTENT', DEFAULT_CLEAR_CONTENT)

        # Create engine
        self.engine = create_engine(engine, echo=engine_echo)

        # Drop tables if we have to
        if drop_all_tables:
            Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

        # Create session
        self.Session = sessionmaker()
        self.Session.configure(bind=self.engine)
        self.session = self.Session()

        # Clear content if we have to
        if clear_content:
            for name, table in Base.metadata.tables.items():
                self.session.execute(table.delete())

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.session.commit()
        self.session.close()
        self.engine.dispose()

    def add_seeds(self, seeds):
        for seed in seeds:
            db_page, _ = self._get_or_create_db_page(seed)
        self.session.commit()

    def get_next_requests(self, max_next_requests, **kwargs):
        query = self.page_model.query(self.session).with_lockmode('update')
        query = query.filter(self.page_model.state == PageMixin.State.NOT_CRAWLED)
        query = self._get_order_by(query)
        if max_next_requests:
            query = query.limit(max_next_requests)
        next_pages = []
        for db_page in query:
            db_page.state = PageMixin.State.QUEUED
            request = self.manager.request_model(url=db_page.url, meta=db_page.meta, headers=db_page.headers,
                                                 cookies=db_page.cookies, method=db_page.method, body=db_page.body)
            next_pages.append(request)
        self.session.commit()
        return next_pages

    def page_crawled(self, response, links):
        db_page, _ = self._get_or_create_db_page(response)
        db_page.state = PageMixin.State.CRAWLED
        db_page.status_code = response.status_code
        for link in links:
            db_page_from_link, created = self._get_or_create_db_page(link)
            if created:
                db_page_from_link.depth = db_page.depth+1
            self.pages_crawled_in_current_batch += 1

        if self.pages_crawled_in_current_batch and self.pages_crawled_in_current_batch > \
                UPDATE_STATUS_AFTER:
            self.session.commit()
            self.pages_crawled_in_current_batch = 0

    def request_error(self, request, error):
        db_page, _ = self._get_or_create_db_page(request)
        db_page.state = PageMixin.State.ERROR
        status = request.meta.get('scrapy_meta', {}).get('error_status', None)
        if status:
            db_page.status_code = status
        db_page.error = error
        logger.exception("Error {error} on url {url}".format(error=error, url=request.url))
        self.session.commit()

    def _create_page(self, obj):
        db_page = self.page_model()
        db_page.fingerprint = obj.meta['fingerprint']
        db_page.state = PageMixin.State.NOT_CRAWLED
        db_page.url = obj.url
        db_page.created_at = datetime.datetime.utcnow()
        db_page.meta = obj.meta
        if not isinstance(obj, frontera_response):
            db_page.headers = obj.headers
            db_page.method = obj.method
            db_page.cookies = obj.cookies
            if obj.method.lower() == 'post':
                db_page.body = obj.body
            db_page.depth = 0

        return db_page

    def _get_or_create_db_page(self, obj):
        if not self._request_exists(obj.meta['fingerprint']):
            db_page = self._create_page(obj)
            self.session.add(db_page)
            self.manager.logger.backend.debug('Creating request %s' % db_page)
            return db_page, True
        else:
            db_page = self.page_model.query(self.session).filter_by(fingerprint=obj.meta['fingerprint']).first()
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