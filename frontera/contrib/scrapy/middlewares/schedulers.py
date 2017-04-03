import logging

from scrapy.exceptions import IgnoreRequest as _IgnoreRequest
from scrapy.spidermiddlewares.httperror import HttpError

from frontera.utils.misc import load_object

logger = logging.getLogger(__name__)


class IgnoreRequest(_IgnoreRequest):

    def __init__(self, *args, **kwargs):
        self.response = kwargs.pop('response')
        super(IgnoreRequest, self).__init__(*args, **kwargs)


class BaseSchedulerMiddleware(object):

    def __init__(self, crawler):
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    @property
    def scheduler(self):
        return self.crawler.engine.slot.scheduler


class SchedulerSpiderMiddleware(BaseSchedulerMiddleware):
    def process_spider_output(self, response, result, spider):
        return self.scheduler.process_spider_output(response, result, spider)

    def process_spider_exception(self, response, exception, spider):
        return self.scheduler.process_exception(response.request, exception, spider)


class SchedulerDownloaderMiddleware(BaseSchedulerMiddleware):
    def process_response(self, request, response, spider):
        consider_status_code_as_error = getattr(spider, 'consider_status_code_as_error', [])
        handle_httpstatus_list = getattr(spider, 'handle_httpstatus_list', []) or \
                                 request.meta.get('handle_httpstatus_list', [])
        status_code = response.status
        if ((status_code not in range(200, 303)) or
                (status_code in consider_status_code_as_error)) \
                and status_code not in handle_httpstatus_list:
            error_msg = "Unhandled http status {0}, Response {1}".format(status_code, response)
            request.meta['error_status'] = status_code
            logger.debug('adding request to request_error: Got status code: %d' % status_code)
            # maybe shouldn't return response after logging erorr
            self.process_exception(request, HttpError(error_msg), spider)
            response.request = request
            raise IgnoreRequest(response=response)
        self._handle_redirect(response)
        return response

    def process_exception(self, request, exception, spider):
        return self.scheduler.process_exception(request, exception, spider)

    def _handle_redirect(self, response):
        allowed_status = (301, 302, 303, 307)
        if 'Location' in response.headers and response.status in allowed_status:
            fingerprint_function = load_object(self.scheduler.frontier.manager.settings.REQUEST_FINGERPRINT_FUNCTION)
            response.meta['frontier_request'].meta.setdefault(
                'redirect_fingerprints', []).append(fingerprint_function(response.request))
