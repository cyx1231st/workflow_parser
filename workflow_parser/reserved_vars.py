# Copyright (c) 2017 Yingxin Cheng
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

COMPONENT = "component"
TARGET = "target"
HOST = "host"
THREAD = "thread"
REQUEST = "request"
KEYWORD = "keyword"
TIME = "time"
SECONDS = "seconds"

ALL_VARS = {COMPONENT, TARGET, HOST, THREAD, REQUEST, KEYWORD, TIME, SECONDS}

TARGET_VARS = {COMPONENT, TARGET, HOST}
LINE_VARS = ALL_VARS - TARGET_VARS
