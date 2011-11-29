// FlumeDataHandlerFactory.java
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

package com.splunk.shep.connector;

import org.apache.log4j.Logger;

public class FlumeDataHandlerFactory implements S2SDataHandlerFactory {
    private BridgeConfig config;

    public FlumeDataHandlerFactory(BridgeConfig config) {
	this.config = config;
    }

    @Override
    public S2SDataHandler createHandler() throws Exception {
	return new FlumeDataHandler(config);
    }
}

class FlumeDataHandler implements S2SDataHandler {
    private BridgeConfig config;
    private DataSink emitter;
    private EventParser eventParser;
    private Logger logger = Logger.getLogger(getClass());

    public FlumeDataHandler(BridgeConfig config) throws Exception {
	this.config = config;
    }

    /**
     * Called where s2s state machine determines that it has decoded whole
     * CowPipelineData
     * 
     * @throws Exception
     * @raw - s2s bytes
     */
    public void s2sDataAvailable(byte[] raw) throws Exception {
	if (eventParser == null) {
	    if (config.useFlume()) {
		System.err.println("creating Flume connection object.");
		emitter = new EventEmitter(config.getOutPort(),
			config.getOutIP());
		emitter.setMaxEventSize(config.getEventSize());
	    } else {
		System.err.println("creating HDFS IO object.");
		emitter = new HdfsIO(config.getOutIP(), Integer.toString(config
			.getOutPort()), config.getOutPath());
		emitter.setFileRollingSize(config.getFileSize());
	    }

	    eventParser = new EventParser(null, emitter, 0);

	    try {
		emitter.start();
	    } catch (Exception e) {
		logger.error(e);
		emitter.close();
		emitter = null;
		throw e;
	    }
	}
	eventParser.processS2SEvent(raw);
	eventParser.reset();
    }

    protected void finalize() {
	if (emitter != null) {
	    emitter.close();
	    emitter = null;
	    eventParser = null;
	}
    }
}
