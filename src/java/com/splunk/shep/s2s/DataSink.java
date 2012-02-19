// DataSink.java
//
// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shep.s2s;

public interface DataSink {
    void start() throws Exception;

    void setName(String name);

    void start(String sinkPath) throws Exception;

    void close();

    void send(byte[] rawBytes, String sourceType, String source, String host,
	    long time) throws Exception;

    void send(String data, String sourceType, String source, String host,
	    long time) throws Exception;

    void send(byte[] rawBytes) throws Exception;

    void setMaxEventSize(long size);

    void setFileRollingSize(long size);
}
