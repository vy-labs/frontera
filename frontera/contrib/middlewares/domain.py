import re

from frontera.core.components import Middleware
from frontera.utils.url import parse_domain_from_url_fast


def parse_domain_info(url, test_mode=False):
    if test_mode:
        match = re.match('([A-Z])\w+', url)
        netloc = name = match.groups()[0] if match else '?'
        scheme = sld = tld = subdomain = '-'
    else:
        netloc, name, scheme, sld, tld, subdomain = parse_domain_from_url_fast(url)
    return {
        'netloc': netloc,
        'name': name,
        'scheme': scheme,
        'sld': sld,
        'tld': tld,
        'subdomain': subdomain,
    }


class DomainMiddleware(Middleware):
    """
    This :class:`Middleware <frontera.core.components.Middleware>` will add a ``domain`` info field for every
    :attr:`Request.meta <frontera.core.models.Request.meta>` and
    :attr:`Response.meta <frontera.core.models.Response.meta>` if is activated.


    ``domain`` object will contains the following fields:

    - **netloc**: URL netloc according to `RFC 1808`_ syntax specifications
    - **name**: Domain name
    - **scheme**: URL scheme
    - **tld**: Top level domain
    - **sld**: Second level domain
    - **subdomain**: URL subdomain(s)

    An example for a :class:`Request <frontera.core.models.Request>` object::

        >>> request.url
        'http://www.scrapinghub.com:8080/this/is/an/url'

        >>> request.meta['domain']
        {
            "name": "scrapinghub.com",
            "netloc": "www.scrapinghub.com",
            "scheme": "http",
            "sld": "scrapinghub",
            "subdomain": "www",
            "tld": "com"
        }

    If :setting:`TEST_MODE` is active, It will accept testing URLs, parsing letter domains::

        >>> request.url
        'A1'

        >>> request.meta['domain']
        {
            "name": "A",
            "netloc": "A",
            "scheme": "-",
            "sld": "-",
            "subdomain": "-",
            "tld": "-"
        }

    .. _`RFC 1808`: http://tools.ietf.org/html/rfc1808.html

    """
    component_name = 'Domain Middleware'

    def __init__(self, manager):
        self.manager = manager

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        for seed in seeds:
            self._add_domain(seed)
        return seeds

    def page_crawled(self, response):
        return self._add_domain(response)

    def links_extracted(self, request, links):
        for link in links:
            self._add_domain(link)
        return self._add_domain(request)

    def request_error(self, request, error):
        return self._add_domain(request)

    def _add_domain(self, obj):
        obj.meta['domain'] = parse_domain_info(obj.url, self.manager.test_mode)
        if 'redirect_urls' in obj.meta:
            obj.meta['redirect_domains'] = [parse_domain_info(url, self.manager.test_mode)
                                            for url in obj.meta['redirect_urls']]
        return obj
