# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: disable=too-many-arguments

"""
Classes and functions for interacting with the Mesos HTTP restful API
"""

import copy
import logging
import requests
import tenacity
import ujson
import urlparse

from mesos.exceptions import HTTPException
from mesos.exceptions import MesosException

METHODS = ["HEAD", "GET", "POST", "PUT", "PATCH", "DELETE"]

REQUEST_JSON_HEADERS = {'Accept': 'application/json'}
REQUEST_GZIP_HEADERS = {'Accept-Encoding': 'gzip'}

BASE_HEADERS = {}
DEFAULT_TIMEOUT = 30
DEFAULT_AUTH = None
DEFAULT_USE_GZIP_ENCODING = None
DEFAULT_MAX_ATTEMPTS = 3


def simple_urljoin(base, other):
    """
    Do a join by rstrip'ing '/' from base and lstrp'ing '/' from other.

    This function is needed since 'urlparse.urljoin' tries to be too
    smart and strips the subpath from the base.

    :type base: str
    :type other: str
    :rtype: str
    """
    try:
        url = '/'.join([base.rstrip('/'), other.lstrip('/')])
    except Exception as exception:
        raise MesosException("Unable to join '{base}' with '{other}': {error}"
                             .format(base=base, other=other, error=exception))

    return url


class Resource(object):
    """
    Encapsulate the context for an HTTP resource.

    Context for an HTTP resource may include properties such as the URL,
    default timeout for connections, default headers to be included in each
    request, and auth.
    """

    SUCCESS_CODES = frozenset(xrange(200, 300))

    def __init__(self,
                 url,
                 base_headers=BASE_HEADERS,
                 default_timeout=DEFAULT_TIMEOUT,
                 default_auth=DEFAULT_AUTH,
                 default_use_gzip_encoding=DEFAULT_USE_GZIP_ENCODING,
                 default_max_attempts=DEFAULT_MAX_ATTEMPTS):
        """
        :param url: URL identifying the resource
        :type url: str
        :param base_headers: base headers to attach to all requests
        :type base_headers: dict[str, str]
        :param default_timeout: default timeout for requests in seconds
        :type default_timeout: float
        :param default_auth: default auth scheme for requests
        :type default_auth: requests.auth.AuthBase
        :param default_use_gzip_encoding: default boolean indicating whether to
                                          pass gzip encoding in the request
                                          headers or not
        :type default use_gzip_encoding: boolean
        :param max_attempts: maximum number of attempts to try for any request
        :type max_attempts: int
        """

        try:
            self.url = urlparse.urlparse(url)
        except Exception as exception:
            raise MesosException("Cannot parse URL '{url}': {error}"
                                 .format(url=url, error=exception))

        self.base_headers = copy.deepcopy(base_headers)
        self.default_timeout = default_timeout
        self.default_auth = default_auth
        self.default_use_gzip_encoding = default_use_gzip_encoding
        self.default_max_attempts = default_max_attempts


    def subresource(self, subpath):
        """
        Return a new Resource object at a subpath of the current resource's URL.

        :param subpath: relative path of the subresource
        :type subpath: str
        :return: Resource at subpath
        :rtype: Resource
        """
        try:
            url = simple_urljoin(self.url.geturl(), subpath)
        except Exception as exception:
            raise MesosException("Unable to join subpath for"
                                 " subresource: {error}"
                                 .format(error=exception))

        return self.__class__(
            url=url,
            base_headers=self.base_headers,
            default_timeout=self.default_timeout,
            default_auth=self.default_auth,
            default_use_gzip_encoding=self.default_use_gzip_encoding,
            default_max_attempts=self.default_max_attempts)

    def _request(self,
                 method,
                 additional_headers=None,
                 timeout=None,
                 auth=None,
                 use_gzip_encoding=None,
                 params=None,
                 **kwargs):
        """
        Make an HTTP request with the given method and an optional timeout.

        :param method: request method
        :type method: str
        :param additional_headers: additional headers to include in the request
        :type additional_headers: dict[str, str]
        :param timeout: timeout in seconds for the request
        :type timeout: float
        :param auth: auth scheme for the request
        :type auth: requests.auth.AuthBase
        :param use_gzip_encoding: boolean indicating whether to
                                  pass gzip encoding in the request
                                  headers or not
        :type use_gzip_encoding: boolean
        :param params: parameters to include in the request
        :type params: str | dict[str, T]
        :param kwargs: additional arguments to pass to requests.request
        :type kwargs: dict[str, T]
        :return: HTTP response
        :rtype: requests.Response
        """
        if method not in METHODS:
            raise MesosException("Unknown HTTP Method '{method}'"
                                 .format(method=method))

        headers = copy.deepcopy(self.base_headers)
        if additional_headers is not None:
            headers.update(self.base_headers)

        if timeout is None:
            timeout = self.default_timeout

        if auth is None:
            auth = self.default_auth

        if use_gzip_encoding is None:
            use_gzip_encoding = self.default_use_gzip_encoding

        if headers and use_gzip_encoding:
            headers.update(REQUEST_GZIP_HEADERS)

        kwargs = copy.deepcopy(kwargs)
        kwargs.update({
            "method" : method,
            "headers" : headers,
            "timeout" : timeout,
            "auth" : auth,
            "params" : params,
            "url" : self.url.geturl()})

        try:
            response = requests.request(**kwargs)
        except Exception as exception:
            raise MesosException("Unable to retrieve resource"
                                 " at '{url}': {error}"
                                 .format(url=self.url.geturl(),
                                         error=exception))

        if response.status_code not in self.SUCCESS_CODES:
            raise MesosException("Unsuccessful status code returned"
                                 " from request at '{url}': {error}"
                                 .format(url=self.url.geturl(),
                                         error=HTTPException(response)))

        return response

    def request(self,
                method,
                additional_headers=None,
                timeout=None,
                auth=None,
                use_gzip_encoding=None,
                max_attempts=None,
                params=None,
                **kwargs):
        """
        Make an HTTP request by calling self._request with backoff retry.

        :param method: request method
        :type method: str
        :param additional_headers: additional headers to include in the request
        :type additional_headers: dict[str, str]
        :param timeout: timeout in seconds
        :type timeout: float
        :param auth: auth scheme for the request
        :type auth: requests.auth.AuthBase
        :param use_gzip_encoding: boolean indicating whether to
                                  pass gzip encoding in the request
                                  headers or not
        :type use_gzip_encoding: boolean
        :param max_attempts: maximum number of attempts to try for any request
        :type max_attempts: int
        :param params: additional params to include in the request
        :type params: str | dict[str, T]
        :param kwargs: additional arguments to pass to requests.request
        :type kwargs: dict[str, T]
        :return: HTTP response
        :rtype: requests.Response
        """

        if max_attempts is None:
            max_attempts = self.default_max_attempts

        request_with_retry = tenacity.retry(
            stop=tenacity.stop_after_attempt(max_attempts),
            wait=tenacity.wait_exponential(),
            retry=tenacity.retry_if_exception_type((
                requests.exceptions.Timeout,
            )),
            reraise=True)(self._request)

        try:
            response = request_with_retry(
                method=method,
                additional_headers=additional_headers,
                timeout=timeout,
                auth=auth,
                use_gzip_encoding=use_gzip_encoding,
                params=params,
                **kwargs)
        except Exception as exception:
            raise MesosException("Unable to request resource with max"
                                 " attempts of '{max_attempts}': {error}"
                                 .format(max_attempts=max_attempts,
                                         error=exception))

        return response

    def request_json(self,
                     method,
                     timeout=None,
                     auth=None,
                     payload=None,
                     decoder=None,
                     params=None,
                     **kwargs):
        """
        Make an HTTP request and deserialize the response as JSON.

        :param method: request method
        :type method: str
        :param timeout: timeout in seconds
        :type timeout: float
        :param payload: json payload in the request
        :type payload: dict[str, T] | str
        :param decoder: decoder for json response
        :type decoder: (dict) -> T
        :param params: additional params to include in the request
        :type params: str | dict[str, T]
        :param kwargs: additional arguments to pass to requests.request
        :type kwargs: dict[str, T]
        :return: JSON response
        :rtype: dict[str, T]
        """
        try:
            response = self.request(method=method,
                                    additional_headers=REQUEST_JSON_HEADERS,
                                    timeout=timeout,
                                    auth=auth,
                                    use_gzip_encoding=False,
                                    params=params,
                                    json=payload,
                                    **kwargs)
        except Exception as exception:
            raise MesosException("Request for JSON resource failed: {error}"
                                 .format(error=exception))

        try:
            json = ujson.loads(response.text)
        except Exception as exception:
            raise MesosException("Unable to load JSON from '{data}': {error}"
                                .format(data=response.text, error=exception))

        if decoder is None:
            return json

        try:
            json = decoder(json)
        except Exception as exception:
            raise MesosException("Unable to decode JSON '{json}': {error}"
                                .format(json=json, error=exception))

        return json

    def get_json(self,
                 timeout=None,
                 decoder=None,
                 params=None):
        """
        Send a GET request for a JSON object.

        :param timeout: timeout in seconds
        :type  timeout: float
        :param decoder: decoder for json response
        :type decoder: (dict) -> T
        :param params: additional params to include in the request
        :type params: str | dict[str, U]
        :rtype: dict[str, U]
        :return: JSON response
        :rtype: dict[str, T]
        """
        return self.request_json(method="GET",
                                 timeout=timeout,
                                 decoder=decoder,
                                 params=params)

    def post_json(self,
                  timeout=None,
                  payload=None,
                  decoder=None,
                  params=None):
        """
        Sends a POST request with a JSON object. Expects JSON in return.

        :param timeout: timeout in seconds
        :type  timeout: float
        :param payload: post data
        :type  payload: dict[str, T] | str
        :param decoder: decoder for json response
        :type decoder: (dict) -> T
        :param params: additional params to include in the request
        :type params: str | dict[str, T]
        :rtype: dict[str, T]
        :return: JSON response
        :rtype: dict[str, T]
        """
        return self.request_json(method="POST",
                                 timeout=timeout,
                                 payload=payload,
                                 decoder=decoder,
                                 params=params)
