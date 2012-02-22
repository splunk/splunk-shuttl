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
package com.splunk.shep.s2s.forwarder;

import org.apache.log4j.Logger;

import com.splunk.shep.s2s.DataSink;

/**
 * 
 * @author kpakkirisamy
 * 
 */
public class ConsoleSink implements DataSink {

    private Logger logger = Logger.getLogger("ConsoleSink");

    public ConsoleSink() {
    }

    @Override
    public void setName(String name) {

    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void start(String sinkPath) throws Exception {
    }

    @Override
    public void close() {
    }

    @Override
    public void send(byte[] rawBytes, String sourceType, String source,
	    String host, long time) throws Exception {
	logger.debug("ConsoleSink: " + "bytes " + new String(rawBytes, "UTF-8"));
	logger.debug("ConsoleSink: " + "bytes " + sourceType + " " + source
		+ " " + host
		+ " " + time);

    }

    @Override
    public void send(String data, String sourceType, String source,
	    String host, long time) throws Exception {
	logger.debug("ConsoleSink: " + "bytes str:" + data);
	logger.debug("ConsoleSink: " + "bytes str" + sourceType + " " + source
		+ " " + host
		+ " " + time);
    }

    @Override
    public void send(byte[] rawBytes) throws Exception {
	logger.debug("ConsoleSink: " + "byte[] "
		+ new String(rawBytes, "UTF-8"));
    }

    @Override
    public void setMaxEventSize(long size) {

    }

    @Override
    public void setFileRollingSize(long size) {
    }

}
