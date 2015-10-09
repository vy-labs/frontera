from scrapy.spidermiddlewares.httperror import HttpError


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
        if (response.status not in range(200, 303)) or (response.status in consider_status_code_as_error):
            error_msg = "Unhandled http status {0}, Response {1}".format(response.status, response)
            request.meta['error_status'] = response.status
            self.scheduler.process_exception(request, HttpError(error_msg), spider)
        return response

    def process_exception(self, request, exception, spider):
        return self.scheduler.process_exception(request, exception, spider)
