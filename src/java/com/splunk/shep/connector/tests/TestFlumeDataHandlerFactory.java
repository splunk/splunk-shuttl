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

package com.splunk.shep.connector.tests;

import com.splunk.shep.connector.EventParser;
import com.splunk.shep.connector.S2SDataHandler;
import com.splunk.shep.connector.S2SDataHandlerFactory;

/**
 * Should be able to plug this into S2SAcceptor ctor and on receiving S2S data,
 * one should be able to see the parsed data on the console. Writing to console
 * is done by EventParser at this time.
 * 
 * @author jkerai
 */
public class TestFlumeDataHandlerFactory implements S2SDataHandlerFactory {
    public TestFlumeDataHandlerFactory() {
    }

    @Override
    public S2SDataHandler createHandler() {
	return new TestFlumeDataHandler();
    }

}

class TestFlumeDataHandler implements S2SDataHandler {
    private EventParser eventParser = new EventParser(null, 0);

    public TestFlumeDataHandler() {
    }

    /**
     * Called where s2s state machine determines that it has decoded whole
     * CowPipelineData
     * 
     * @raw - s2s bytes
     */
    public void s2sDataAvailable(byte[] raw) {
	eventParser.processS2SEvent(raw);
	eventParser.reset();
    }
}
