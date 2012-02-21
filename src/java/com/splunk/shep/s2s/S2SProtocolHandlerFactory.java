// S2SV43DataHandlerFactory.java
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

import org.apache.log4j.Logger;

public class S2SProtocolHandlerFactory {
    public static String VERSION_43 = "4.3.*";

    public static S2SProtocolHandler createHandler(String version)
	    throws Exception {
	if (VERSION_43.equals(version)) {
	    return new S2SV43DataHandler();
	} else {
	    throw new Exception("Version not supported");
	}
    }
}

class S2SV43DataHandler implements S2SProtocolHandler {
    private DataSink sink;
    private S2SEventParser eventParser;
    private Logger logger = Logger.getLogger(getClass());

    public S2SV43DataHandler() throws Exception {
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
	    eventParser = new S2SEventParser(null, sink, 0);
	    try {
		sink.start();
	    } catch (Exception e) {
		logger.error(e);
		sink.close();
		sink = null;
		throw e;
	    }
	}
	eventParser.processS2SEvent(raw);
	eventParser.reset();
    }

    public DataSink getSink() {
	return sink;
    }

    public void setSink(DataSink sink) {
	this.sink = sink;
    }

    protected void finalize() {
	if (sink != null) {
	    sink.close();
	    sink = null;
	    eventParser = null;
	}
    }
}
