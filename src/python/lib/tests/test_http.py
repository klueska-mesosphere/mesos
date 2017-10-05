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

from __future__ import absolute_import

from collections import namedtuple

import mock
import pytest
import requests.exceptions
import ujson

from mesos import http


def test_resource():
    # test initialization
    resource = http.Resource('http://some.domain/some/prefix')
    assert resource.url.geturl() == 'http://some.domain/some/prefix'

    # test get_resource_at_subpath
    new_rc = resource.get_subresource('some/subpath')
    assert new_rc.url.geturl() == 'http://some.domain/some/prefix/some/subpath'


@pytest.mark.parametrize('default_tos,override_tos,expected_request_calls', [
    (None, None, [
        mock.call(auth=None, headers={'Content-Type': 'applicatioin/json',
                                      'some-header-key': 'some-header-val'},
                  method='GET', params=None, somekey='someval', timeout=None,
                  url='http://some.domain/some/prefix')]),
    (10, None, [
        mock.call(auth=None, headers={'Content-Type': 'applicatioin/json',
                                      'some-header-key': 'some-header-val'},
                  method='GET', params=None, somekey='someval', timeout=10,
                  url='http://some.domain/some/prefix')]),
    (10, 100, [mock.call(auth=None,
                         headers={'Content-Type': 'applicatioin/json',
                                  'some-header-key': 'some-header-val'},
                         method='GET', params=None, somekey='someval',
                         timeout=100,
                         url='http://some.domain/some/prefix')]),
])
def test_resource_request(mock_mesos_http_request, default_tos, override_tos,
                          expected_request_calls):
    # test request
    mock_mesos_http_request.return_value = 'some_return_value'
    resource = http.Resource('http://some.domain/some/prefix',
                             timeout_secs=default_tos,
                             headers={'Content-Type': 'applicatioin/json'})
    ret = resource.request(http.METHOD_GET, override_tos, somekey='someval',
                           additional_headers={
                               'some-header-key': 'some-header-val'})
    assert ret == 'some_return_value'
    assert mock_mesos_http_request.mock_calls == expected_request_calls


SomeModel = namedtuple('SomeModel', ['some'])


@mock.patch('mesos.http.ujson.loads')
@pytest.mark.parametrize(
    [
        'default_tos',
        'override_tos',
        'json_payload',
        'obj_decoder',
        'request_exception',
        'resp_status',
        'json_exception',
        'expected_exception',
        'expected_additional_kwargs',
    ],
    [
        (None, None, {'some': 'payload'}, None, None, 200, None,
         None, [{}]),
        (None, None, {'some': 'payload'}, lambda d: SomeModel(**d), None, 200,
         None, None, [{}]),
        (None, None, {'some': 'payload'}, None,
         requests.exceptions.SSLError, 200, None,
         http.HTTPException, [{}]),
        (None, None, {'some': 'payload'}, None,
         requests.exceptions.ConnectionError, 200, None,
         http.HTTPException, [{}]),
        (None, None, {'some': 'payload'}, None,
         requests.exceptions.Timeout, 200, None,
         http.HTTPException, [{}]),
        (None, None, {'some': 'payload'}, None,
         requests.exceptions.RequestException, 200, None,
         http.HTTPException, [{}]),
        (None, None, {'some': 'payload'}, None, None, 299, None,
         None, [{}]),
        (None, None, {'some': 'payload'}, None, None, 200,
         ValueError, http.HTTPException, [{}]),
        (10, None, {'some': 'payload'}, None, None, 200, None,
         None, [dict(timeout=10)]),
        (10, 100, {'some': 'payload'}, None, None, 200, None,
         None, [dict(timeout=100)]),
        (None, None, {'some': 'payload'}, None, None, 200, None,
         None, [{}]),
        (10, 100, {'some': 'payload'}, None, None, 400, None,
         http.MesosResource.ERROR_CODE_MAP[400],
         [dict(timeout=100)]),
        (10, 100, {'some': 'payload'}, None, None, 500, None,
         http.HTTPException,
         [dict(timeout=100)]),
    ]
)
def test_mesos_rc_request_json(
        mock_ujson_loads,
        mock_mesos_http_request,
        default_tos,
        override_tos,
        json_payload,
        obj_decoder,
        request_exception,
        resp_status,
        json_exception,
        expected_exception,
        expected_additional_kwargs
):
    call_kwargs = dict(method=http.METHOD_POST,
                       json={'some': 'payload'},
                       timeout=None,
                       url='http://some.domain/some/prefix',
                       auth=None,
                       headers={'Accept': 'application/json',
                                'Accept-Encoding': 'gzip'},
                       params=None)
    mock_calls = []
    for kwargs in expected_additional_kwargs:
        call_kwargs.update(kwargs)
        mock_calls.append(mock.call(**call_kwargs))

    def json_side_effect(_):
        if json_exception is None:
            return {'some': 'return_value'}
        else:
            raise json_exception

    def request_side_effect(*_, **__):
        if request_exception is None:
            return mock.Mock(status_code=resp_status)
        else:
            raise request_exception

    mock_mesos_http_request.side_effect = request_side_effect
    mock_ujson_loads.side_effect = json_side_effect

    mrc = http.MesosResource('http://some.domain/some/prefix',
                             default_timeout_secs=default_tos, auth=None,
                             num_attempts=1)

    if expected_exception is None:
        ret = mrc.request_json(http.METHOD_POST, timeout_secs=override_tos,
                               payload=json_payload,
                               decoder=obj_decoder)
        expected_ret = {'some': 'return_value'}
        if obj_decoder is None:
            assert ret == expected_ret
        else:
            assert ret == SomeModel(**expected_ret)
    else:
        with pytest.raises(expected_exception):
            mrc.request_json(http.METHOD_POST, timeout_secs=override_tos,
                             payload=json_payload)

    assert mock_mesos_http_request.mock_calls == mock_calls


def test_mesos_rc_get_json(mock_mesos_http_request):
    mock_mesos_http_request.return_value = mock.Mock(
        status_code=200,
        text=ujson.dumps({'hello': 'world'}),
    )
    mock_auth = mock.Mock()
    mrc = http.MesosResource('http://some.domain/some/prefix',
                             default_timeout_secs=100, auth=mock_auth,
                             num_attempts=1)
    ret = mrc.get_json()
    assert mock_mesos_http_request.mock_calls == [
        mock.call(
            json=None,
            method='GET',
            url='http://some.domain/some/prefix',
            auth=mock_auth,
            headers={'Accept-Encoding': 'gzip',
                     'Accept': 'application/json'},
            params=None,
            timeout=None,
        )
    ]
    assert ret == {'hello': 'world'}


def test_mesos_rc_post_json(mock_mesos_http_request):
    mock_mesos_http_request.return_value = mock.Mock(
        status_code=200,
        text=ujson.dumps({'hello': 'world'}),
    )
    mock_auth = mock.Mock()
    mrc = http.MesosResource('http://some.domain/some/prefix',
                             default_timeout_secs=100, auth=mock_auth,
                             num_attempts=1)
    ret = mrc.post_json(payload={'somekey': 'someval'})
    assert mock_mesos_http_request.mock_calls == [
        mock.call(
            json={'somekey': 'someval'},
            method='POST',
            url='http://some.domain/some/prefix',
            auth=mock_auth,
            headers={'Accept-Encoding': 'gzip',
                     'Accept': 'application/json'},
            params=None,
            timeout=None,
        )
    ]
    assert ret == {'hello': 'world'}
