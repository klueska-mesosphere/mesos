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

# pylint: disable=missing-docstring,protected-access,too-many-locals,too-many-arguments

import mock
import pytest
import requests
import ujson

from collections import namedtuple

from mesos import http

from mesos.exceptions import MesosException


def test_resource_creation():
    try:
        resource = http.Resource({})
    except Exception as exception:
        assert type(exception) is MesosException
        assert str(exception).startswith("Cannot parse URL '{}': ")

    resource = http.Resource("http://some.domain/some/prefix")
    assert resource.url.geturl() == "http://some.domain/some/prefix"

    try:
        resource = resource.subresource({})
    except Exception as exception:
        assert type(exception) is MesosException
        assert str(exception).startswith(
            "Unable to join subpath for subresource: ")

    resource = resource.subresource("/subpath")
    assert resource.url.geturl() == "http://some.domain/some/prefix/subpath"


@pytest.mark.parametrize(
    "status_code, resource_kwargs, request_kwargs, mock_call_or_exception",
    [(400,
      dict(url="http://some.domain/some/prefix"),
      dict(method="BOGUS"),
      MesosException("Unable to request resource with max attempts"
                     " of '{max_attempts}': Unknown HTTP Method 'BOGUS'"
                     .format(max_attempts=http.DEFAULT_MAX_ATTEMPTS))),

     (400,
      dict(url="http://some.domain/some/prefix",
           default_max_attempts=10),
      dict(method="BOGUS"),
      MesosException("Unable to request resource with max attempts"
                     " of '{max_attempts}': Unknown HTTP Method 'BOGUS'"
                     .format(max_attempts=10))),

     (400,
      dict(url="http://some.domain/some/prefix"),
      dict(method="GET"),
      MesosException("Unsuccessful status code returned from request"
                     " at 'http://some.domain/some/prefix': ")),

     (200,
      dict(url="http://some.domain/some/prefix"),
      dict(method="GET"),
      mock.call(method="GET",
                url='http://some.domain/some/prefix',
                headers=http.BASE_HEADERS,
                timeout=http.DEFAULT_TIMEOUT,
                auth=http.DEFAULT_AUTH,
                params=None)),

     (200,
      dict(url="http://some.domain/some/prefix",
           base_headers={"Content-Type": "application/json"}),
      dict(method="GET"),
      mock.call(method="GET",
                url='http://some.domain/some/prefix',
                headers={"Content-Type": "application/json"},
                timeout=http.DEFAULT_TIMEOUT,
                auth=http.DEFAULT_AUTH,
                params=None)),

     (200,
      dict(url="http://some.domain/some/prefix",
           base_headers={"Content-Type": "application/json"},
           default_timeout=60),
      dict(method="GET"),
      mock.call(method="GET",
                url='http://some.domain/some/prefix',
                headers={"Content-Type": "application/json"},
                timeout=60,
                auth=http.DEFAULT_AUTH,
                params=None)),

     (200,
      dict(url="http://some.domain/some/prefix",
           base_headers={"Content-Type": "application/json"},
           default_timeout=60,
           default_auth="A Different Auth"),
      dict(method="GET"),
      mock.call(method="GET",
                url='http://some.domain/some/prefix',
                headers={"Content-Type": "application/json"},
                timeout=60,
                auth="A Different Auth",
                params=None)),

     (200,
      dict(url="http://some.domain/some/prefix",
           base_headers={"Content-Type": "application/json"},
           default_timeout=60,
           default_auth="A Different Auth",
           default_use_gzip_encoding=False),
      dict(method="GET"),
      mock.call(method="GET",
                url='http://some.domain/some/prefix',
                headers={"Content-Type": "application/json"},
                timeout=60,
                auth="A Different Auth",
                params=None))])
def test_resource_request(mock_mesos_http_request,
                          status_code,
                          resource_kwargs,
                          request_kwargs,
                          mock_call_or_exception):
    mock_mesos_http_request.return_value = requests.Response
    mock_mesos_http_request.return_value.status_code = status_code
    mock_mesos_http_request.return_value.text = "Response Text"

    try:
        resource = http.Resource(**resource_kwargs)
    except Exception as exception:
        assert type(exception) is type(mock_call_or_exception)
        assert str(exception).find(str(mock_call_or_exception)) >= 0
        return

    try:
        response = resource.request(**request_kwargs)
    except Exception as exception:
        assert type(exception) is type(mock_call_or_exception)
        assert str(exception).find(str(mock_call_or_exception)) >= 0
        return

    assert response == mock_mesos_http_request.return_value
    assert mock_mesos_http_request.mock_calls[0] == mock_call_or_exception

    #resource_kwargs = dict(url="http://some.domain/some/prefix",
    #                       base_headers="{'Content-Type': 'application/json'}")
    #request_kwargs = dict(method="GET")
    #assert_request(mock.call(method="GET"), resource_kwargs, request_kwargs)

    #resource_kwargs = dict(url="http://some.domain/some/prefix",
    #                       base_headers="{'Content-Type': 'application/json'}")
    #request_kwargs = dict(method="GET", timeout=5)
    #assert_request(mock.call(method="GET"), resource_kwargs, request_kwargs)
    #resource_kwargs = dict(url="http://some.domain/some/prefix",
    #                       base_headers={'Content-Type': 'application/json'})

    #resource_kwargs = dict(url="http://some.domain/some/prefix",
    #                       base_headers={'Content-Type': 'application/json'},
    #                       default_timeout=default_timeout,
    #                       default_auth=auth,
    #                       default_use_gzip_encoding=use_gzip_encoding,
    #                       default_max_attempts=default_max_attempts)
    #request = mock.call(method="GET",
    #                    additional_headers =
    #    auth=None, headers=
    #assert_request(
#
#
#SomeModel = namedtuple('SomeModel', ['some'])
#
#
#@mock.patch('mesos.http.ujson.loads')
#@pytest.mark.parametrize(
#    [
#        'default_tos',
#        'override_tos',
#        'json_payload',
#        'obj_decoder',
#        'request_exception',
#        'resp_status',
#        'json_exception',
#        'expected_exception',
#        'expected_additional_kwargs',
#    ],
#    [
#        (None, None, {'some': 'payload'}, None, None, 200, None,
#         None, [{}]),
#        (None, None, {'some': 'payload'}, lambda d: SomeModel(**d), None, 200,
#         None, None, [{}]),
#        (None, None, {'some': 'payload'}, None,
#         requests.exceptions.SSLError, 200, None,
#         http.HTTPException, [{}]),
#        (None, None, {'some': 'payload'}, None,
#         requests.exceptions.ConnectionError, 200, None,
#         http.HTTPException, [{}]),
#        (None, None, {'some': 'payload'}, None,
#         requests.exceptions.Timeout, 200, None,
#         http.HTTPException, [{}]),
#        (None, None, {'some': 'payload'}, None,
#         requests.exceptions.RequestException, 200, None,
#         http.HTTPException, [{}]),
#        (None, None, {'some': 'payload'}, None, None, 299, None,
#         None, [{}]),
#        (None, None, {'some': 'payload'}, None, None, 200,
#         ValueError, http.HTTPException, [{}]),
#        (10, None, {'some': 'payload'}, None, None, 200, None,
#         None, [dict(timeout=10)]),
#        (10, 100, {'some': 'payload'}, None, None, 200, None,
#         None, [dict(timeout=100)]),
#        (None, None, {'some': 'payload'}, None, None, 200, None,
#         None, [{}]),
#        (10, 100, {'some': 'payload'}, None, None, 400, None,
#         http.MesosResource.ERROR_CODE_MAP[400],
#         [dict(timeout=100)]),
#        (10, 100, {'some': 'payload'}, None, None, 500, None,
#         http.HTTPException,
#         [dict(timeout=100)]),
#    ]
#)
#def test_mesos_rc_request_json(
#        mock_ujson_loads,
#        mock_mesos_http_request,
#        default_tos,
#        override_tos,
#        json_payload,
#        obj_decoder,
#        request_exception,
#        resp_status,
#        json_exception,
#        expected_exception,
#        expected_additional_kwargs
#):
#    call_kwargs = dict(method=http.METHOD_POST,
#                       json={'some': 'payload'},
#                       timeout=None,
#                       url='http://some.domain/some/prefix',
#                       auth=None,
#                       headers={'Accept': 'application/json',
#                                'Accept-Encoding': 'gzip'},
#                       params=None)
#    mock_calls = []
#    for kwargs in expected_additional_kwargs:
#        call_kwargs.update(kwargs)
#        mock_calls.append(mock.call(**call_kwargs))
#
#    def json_side_effect(_):
#        if json_exception is None:
#            return {'some': 'return_value'}
#        else:
#            raise json_exception
#
#    def request_side_effect(*_, **__):
#        if request_exception is None:
#            return mock.Mock(status_code=resp_status)
#        else:
#            raise request_exception
#
#    mock_mesos_http_request.side_effect = request_side_effect
#    mock_ujson_loads.side_effect = json_side_effect
#
#    mrc = http.MesosResource('http://some.domain/some/prefix',
#                             default_timeout_secs=default_tos, auth=None,
#                             num_attempts=1)
#
#    if expected_exception is None:
#        ret = mrc.request_json(http.METHOD_POST, timeout_secs=override_tos,
#                               payload=json_payload,
#                               decoder=obj_decoder)
#        expected_ret = {'some': 'return_value'}
#        if obj_decoder is None:
#            assert ret == expected_ret
#        else:
#            assert ret == SomeModel(**expected_ret)
#    else:
#        with pytest.raises(expected_exception):
#            mrc.request_json(http.METHOD_POST, timeout_secs=override_tos,
#                             payload=json_payload)
#
#    assert mock_mesos_http_request.mock_calls == mock_calls
#
#
#def test_mesos_rc_get_json(mock_mesos_http_request):
#    mock_mesos_http_request.return_value = mock.Mock(
#        status_code=200,
#        text=ujson.dumps({'hello': 'world'}),
#    )
#    mock_auth = mock.Mock()
#    mrc = http.MesosResource('http://some.domain/some/prefix',
#                             default_timeout_secs=100, auth=mock_auth,
#                             num_attempts=1)
#    ret = mrc.get_json()
#    assert mock_mesos_http_request.mock_calls == [
#        mock.call(
#            json=None,
#            method='GET',
#            url='http://some.domain/some/prefix',
#            auth=mock_auth,
#            headers={'Accept-Encoding': 'gzip',
#                     'Accept': 'application/json'},
#            params=None,
#            timeout=None,
#        )
#    ]
#    assert ret == {'hello': 'world'}
#
#
#def test_mesos_rc_post_json(mock_mesos_http_request):
#    mock_mesos_http_request.return_value = mock.Mock(
#        status_code=200,
#        text=ujson.dumps({'hello': 'world'}),
#    )
#    mock_auth = mock.Mock()
#    mrc = http.MesosResource('http://some.domain/some/prefix',
#                             default_timeout_secs=100, auth=mock_auth,
#                             num_attempts=1)
#    ret = mrc.post_json(payload={'somekey': 'someval'})
#    assert mock_mesos_http_request.mock_calls == [
#        mock.call(
#            json={'somekey': 'someval'},
#            method='POST',
#            url='http://some.domain/some/prefix',
#            auth=mock_auth,
#            headers={'Accept-Encoding': 'gzip',
#                     'Accept': 'application/json'},
#            params=None,
#            timeout=None,
#        )
#    ]
#    assert ret == {'hello': 'world'}
