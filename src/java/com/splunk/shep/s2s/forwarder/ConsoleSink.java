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

    private Logger logger = Logger.getLogger(getClass());

    public ConsoleSink() {
	logger.info("init");
    }

    @Override
    public void setName(String name) {

    }

    @Override
    public void start() throws Exception {
	// TODO Auto-generated method stub
	logger.info("start");
    }

    @Override
    public void start(String sinkPath) throws Exception {
	// TODO Auto-generated method stub
	logger.info("start");
    }

    @Override
    public void close() {
	// TODO Auto-generated method stub
	logger.info("close");
    }

    @Override
    public void send(byte[] rawBytes, String sourceType, String source,
	    String host, long time) throws Exception {
	// TODO Auto-generated method stub
	System.out.println("bytes " + new String(rawBytes, "UTF-8"));
	System.out.println("bytes " + sourceType + " " + source + " " + host
		+ " " + time);

    }

    @Override
    public void send(String data, String sourceType, String source,
	    String host, long time) throws Exception {
	// TODO Auto-generated method stub
	System.out.println("bytes str:" + data);
	System.out.println("bytes str" + sourceType + " " + source + " " + host
		+ " " + time);
    }

    @Override
    public void send(byte[] rawBytes) throws Exception {
	// TODO Auto-generated method stub
	System.out.println("byte[] " + new String(rawBytes, "UTF-8"));

    }

    @Override
    public void setMaxEventSize(long size) {
	// TODO Auto-generated method stub

    }

    @Override
    public void setFileRollingSize(long size) {
	// TODO Auto-generated method stub

    }

}
