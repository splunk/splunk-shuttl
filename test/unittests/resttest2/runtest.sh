#!/bin/sh

# start.sh
#
# Copyright (C) 2011 Splunk Inc.
#
# Splunk Inc. licenses this file
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
#

script_dir=$(dirname $0)

expected_rest_out="localhost"
actual_rest_out=`curl -s http://localhost:9090/shep/rest/server/defaulthost`

# Output
if [ "$expected_rest_out" != "$actual_rest_out" ]
then
  echo "Fail!
  Expected:
  \"$expected_rest_out\"
  Actual:
  \"$actual_rest_out\""
  exit 1
else
  exit 0
fi
