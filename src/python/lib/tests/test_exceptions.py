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

# pylint: disable=redefined-outer-name,missing-docstring

import mock
import pytest

from mesos.exceptions import HTTPException


@pytest.mark.parametrize("exception, exception_string", [
    (HTTPException, "HTTP 400: Something bad happened")
])
def test_exceptions(exception, exception_string):
    """
    Test exceptions
    """
    response = mock.Mock()
    response.status_code = 400
    response.reason = "some_reason"
    response.request.url = "http://some.url"
    response.text = "Something bad happened"

    error = exception(response)
    assert str(error) == exception_string
    assert error.status() == 400
