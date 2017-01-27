import hashlib
from urlparse import urlparse
from zlib import crc32
from struct import pack
from binascii import hexlify
from w3lib.url import canonicalize_url
from w3lib.util import to_bytes


def sha1(key):
    return hashlib.sha1(key.encode('utf8')).hexdigest()


def md5(key):
    return hashlib.md5(key.encode('utf8')).hexdigest()


def hostname_local_fingerprint(key):
    result = urlparse(key)
    if not result.hostname:
        return sha1(key)
    host_checksum = crc32(result.hostname) if type(result.hostname) is str else \
        crc32(result.hostname.encode('utf-8', 'ignore'))
    doc_uri_combined = result.path+';'+result.params+result.query+result.fragment

    doc_uri_combined = doc_uri_combined if type(doc_uri_combined) is str else \
        doc_uri_combined.encode('utf-8', 'ignore')
    doc_fprint = hashlib.md5(doc_uri_combined).digest()
    fprint = hexlify(pack(">i16s", host_checksum, doc_fprint))
    return fprint


def request_fingerprint(request, include_headers=None):
    """
    Return the request fingerprint.
    The request fingerprint is a hash that uniquely identifies the resource the
    request points to. For example, take the following two urls:
    http://www.example.com/query?id=111&cat=222
    http://www.example.com/query?cat=222&id=111
    Even though those are two different URLs both point to the same resource
    and are equivalent (ie. they should return the same response).
    Another example are cookies used to store session ids. Suppose the
    following page is only accesible to authenticated users:
    http://www.example.com/members/offers.html
    Lot of sites use a cookie to store the session id, which adds a random
    component to the HTTP Request and thus should be ignored when calculating
    the fingerprint.
    For this reason, request headers are ignored by default when calculating
    the fingeprint. If you want to include specific headers use the
    include_headers argument, which is a list of Request headers to include.
    """
    if include_headers:
        include_headers = tuple(to_bytes(h.lower())
                                for h in sorted(include_headers))

    fp = hashlib.sha1()
    fp.update(to_bytes(request.method))
    fp.update(to_bytes(canonicalize_url(request.url)))
    fp.update(request.meta.get('scrapy_body', b'') or request.body or b'')
    if include_headers:
        for hdr in include_headers:
            if hdr in request.headers:
                fp.update(hdr)
                for v in request.headers.getlist(hdr):
                    fp.update(v)
    return fp.hexdigest()
