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

package com.splunk.connector.tests;

import com.splunk.connector.BridgeConfig;
import com.splunk.connector.FlumeDataHandlerFactory;
import com.splunk.connector.S2SAcceptor;
import com.splunk.connector.Splunk2Flume;

public class Splunk2Flume2ConsoleTest extends Splunk2Flume {
	public static void main(String[] args) throws Exception {
		Splunk2Flume2ConsoleTest s2f2ct = new Splunk2Flume2ConsoleTest();
		s2f2ct.parseConfig(args);
		s2f2ct.createAcceptors();
		s2f2ct.run();
	}

	protected void createAcceptors() {
		for (BridgeConfig conf : configs) {
			acceptors.add(new S2SAcceptor(conf, new TestFlumeDataHandlerFactory()));
		}
	}
}
