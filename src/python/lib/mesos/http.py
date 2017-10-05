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

from __future__ import absolute_import

import logging
import urlparse

# requests must be imported after all native packages
import requests
import tenacity
import ujson

from mesos.exceptions import (HTTPException, MesosAuthenticationException,
                              MesosAuthorizationException, MesosBadRequest,
                              MesosUnprocessableException)

METHOD_HEAD = 'HEAD'
METHOD_GET = 'GET'
METHOD_POST = 'POST'
METHOD_PUT = 'PUT'
METHOD_PATCH = 'PATCH'
METHOD_DELETE = 'DELETE'

REQUEST_JSON_HEADERS = {'Accept': 'application/json'}
REQUEST_GZIP_HEADERS = {'Accept-Encoding': 'gzip'}

LOGGER = logging.getLogger(__name__)


def simple_url_join(base_url, other_url):
    """
    Do a join by rstrip'ing / from base_url and lstrp'ing / from other_url.

    This is needed since urlparse.urljoin tries to be too smart
    and strips the subpath from base_url.

    :type base_url: str
    :type other_url: str
    :rtype: str
    """
    return '/'.join([base_url.rstrip('/'), other_url.lstrip('/')])


class Resource(object):
    """
    Encapsulate the context for an HTTP resource.

    Context for an HTTP resource may include properties such as the URL,
    default timeout for connections, default headers to be included in each
    request, and auth.
    """

    def __init__(self, url, headers=None, timeout_secs=None, auth=None):
        """
        :param url: URL identifying the resource
        :type url: str
        :param timeout_secs: timeout in seconds
        :type timeout_secs: float
        :param headers: headers to attache to requests
        :type headers: dict[str, str]
        :param auth: auth scheme
        :type auth: requests.auth.AuthBase
        """
        self.url = urlparse.urlparse(url)
        self._timeout_secs = timeout_secs
        self._auth = auth

        # Init default headers
        header_items = ()
        if headers is not None:
            header_items = headers.iteritems()
        self._default_header_items = frozenset(header_items)

    def _default_headers(self):
        """
        Return a copy of the default headers.

        :rtype: dict[str, str]
        """
        return dict(self._default_header_items)

    def get_subresource(self, subpath):
        """
        Return a new Resource object at a subpath of the current resource's URL.

        :param subpath: subpath of the resource
        :type subpath: str
        :return: Resource at subpath
        :rtype: Resource
        """
        return self.__class__(
            url=simple_url_join(self.url.geturl(), subpath),
            headers=self._default_headers(),
            timeout_secs=self._timeout_secs,
            auth=self._auth,
        )

    def request(self,
                method,
                timeout_secs=None,
                additional_headers=None,
                params=None,
                **kwargs):
        """
        Make an HTTP request with given method and an optional timeout.

        :param method: request method
        :type method: str
        :param timeout_secs: timeout in seconds
        :type timeout_secs: float
        :param additional_headers: additional headers to include in the request
        :type additional_headers: dict[str, str]
        :param params: additional params to include in the request
        :type params: str | dict[str, T]
        :param kwargs: additional arguments to pass to requests.request
        :type kwargs: dict[str, T]
        :return: HTTP response
        :rtype: requests.Response
        """
        if timeout_secs is None:
            timeout_secs = self._timeout_secs

        headers = self._default_headers()
        if additional_headers is not None:
            headers.update(additional_headers)

        LOGGER.info(
            'Sending HTTP %r to %r: %r',
            method,
            self.url,
            headers)

        request_kwargs = dict(
            method=method,
            url=self.url.geturl(),
            headers=headers,
            params=params,
            timeout=timeout_secs,
            auth=self._auth,
        )
        request_kwargs.update(kwargs)

        return requests.request(**request_kwargs)

    def request_json(self,
                     method,
                     timeout_secs=None,
                     payload=None,
                     decoder=None,
                     params=None,
                     **kwargs):
        """
        Make an HTTP request and deserialize the response as JSON.

        :param method: request method
        :type method: str
        :param timeout_secs: timeout in seconds
        :type timeout_secs: float
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
        resp = self.request(method=method,
                            timeout_secs=timeout_secs,
                            json=payload,
                            additional_headers=REQUEST_JSON_HEADERS,
                            params=params,
                            **kwargs)

        try:
            obj = ujson.loads(resp.text)
        except ValueError as exception:
            raise HTTPException('Could not load JSON from "{data}": {error}'
                                .format(data=resp.text, error=str(exception)))

        if decoder is not None:
            return decoder(obj)
        return obj

    def get_json(self, timeout_secs=None, decoder=None, params=None):
        """
        Send a GET request.

        :param timeout_secs: timeout in seconds
        :type  timeout_secs: float
        :param decoder: decoder for json response
        :type decoder: (dict) -> T
        :param params: additional params to include in the request
        :type params: str | dict[str, U]
        :rtype: dict[str, U]
        """
        return self.request_json(METHOD_GET,
                                 timeout=timeout_secs,
                                 decoder=decoder,
                                 params=params)

    def post_json(self,
                  timeout_secs=None,
                  payload=None,
                  decoder=None,
                  params=None):
        """
        Sends a POST request.

        :param timeout_secs: timeout in seconds
        :type  timeout_secs: float
        :param payload: post data
        :type  payload: dict[str, T] | str
        :param decoder: decoder for json response
        :type decoder: (dict) -> T
        :param params: additional params to include in the request
        :type params: str | dict[str, T]
        :rtype: dict[str, T]
        """
        return self.request_json(METHOD_POST,
                                 timeout=timeout_secs,
                                 payload=payload,
                                 decoder=decoder,
                                 params=params)


class MesosResource(Resource):
    """
    Class adding the context necessary to talk to Mesos master and agent APIs.
    """

    SUCCESS_CODE_SET = frozenset(xrange(200, 300))
    ERROR_CODE_MAP = {
        400: MesosBadRequest,
        401: MesosAuthenticationException,
        403: MesosAuthorizationException,
        422: MesosUnprocessableException,
    }

    def __init__(self,
                 url,
                 default_headers=None,
                 default_timeout_secs=None,
                 auth=None,
                 use_gzip_encoding=True,
                 num_attempts=3):
        """
        :param url: URL identifying the resource
        :type url: str
        :param default_timeout_secs: timeout in seconds
        :type default_timeout_secs: float
        :param default_headers: headers to attache to requests
        :type default_headers: dict[str, str]
        :param auth: auth scheme
        :type auth: requests.auth.AuthBase
        """
        super(MesosResource, self).__init__(
            url=url,
            headers=default_headers,
            timeout_secs=default_timeout_secs,
            auth=auth,
        )
        self._use_gzip_encoding = use_gzip_encoding
        self._num_attempts = num_attempts

    def _request(self,
                 method,
                 timeout_secs=None,
                 additional_headers=None,
                 params=None,
                 **kwargs):
        """
        Make an HTTP request with verb 'method' and an optional timeout.

        Handle the response according to the returned status code, raising
        corresponding subclasses of MesosException.

        :param method: request method
        :type method: str
        :param timeout_secs: timeout in seconds
        :type timeout_secs: float
        :param additional_headers: additional headers to include in the request
        :type additional_headers: dict[str, str]
        :param params: additional params to include in the request
        :type params: str | dict[str, T]
        :param kwargs: additional arguments to pass to requests.request
        :type kwargs: dict[str, T]
        :return: HTTP response
        :rtype: requests.Response
        """
        headers = {}

        if self._use_gzip_encoding:
            headers.update(REQUEST_GZIP_HEADERS)

        if additional_headers is not None:
            headers.update(additional_headers)

        resp = super(MesosResource, self).request(
            method=method,
            timeout_secs=timeout_secs,
            additional_headers=headers,
            params=params,
            **kwargs
        )

        # Handle response
        if resp.status_code in self.SUCCESS_CODE_SET:
            return resp
        if resp.status_code in self.ERROR_CODE_MAP:
            raise self.ERROR_CODE_MAP[resp.status_code](resp)

        # Otherwise we got a response we don't know how to handle,
        # raise HTTP exception
        raise HTTPException(resp)

    def request(self,
                method,
                timeout_secs=None,
                additional_headers=None,
                params=None,
                **kwargs):
        """
        Make an HTTP request by calling self._request with backoff retry.

        :param method: request method
        :type method: str
        :param timeout_secs: timeout in seconds, overrides default_timeout_secs
        :type timeout_secs: float
        :param additional_headers: additional headers to include in the request
        :type additional_headers: dict[str, str]
        :param params: additional params to include in the request
        :type params: str | dict[str, T]
        :param kwargs: additional arguments to pass to requests.request
        :type kwargs: dict[str, T]
        :return: HTTP response
        :rtype: requests.Response
        """
        request_with_retry = tenacity.retry(
            stop=tenacity.stop_after_attempt(self._num_attempts),
            wait=tenacity.wait_exponential(),
            retry=tenacity.retry_if_exception_type((
                requests.exceptions.Timeout,
            )),
            reraise=True,
        )(self._request)

        try:
            return request_with_retry(
                method=method,
                timeout_secs=timeout_secs,
                additional_headers=additional_headers,
                params=params,
                **kwargs
            )
        except requests.exceptions.SSLError as err:
            LOGGER.exception('HTTP SSL Error')
            raise HTTPException('An SSL error occurred: {err}'.format(err=err))
        except requests.exceptions.ConnectionError as err:
            LOGGER.exception('HTTP Connection Error')
            raise HTTPException('URL {url} is unreachable: {err}'
                                .format(url=self.url.geturl(), err=err))
        except requests.exceptions.Timeout:
            LOGGER.exception('HTTP Timeout')
            raise HTTPException('Request to URL {url} timed out.'
                                .format(url=self.url))
        except requests.exceptions.RequestException as err:
            LOGGER.exception('HTTP Exception')
            raise HTTPException('HTTP Exception: {err}'.format(err=err))
