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

import java.io.FileInputStream;
import java.io.IOException;
import org.apache.log4j.Logger;

import com.splunk.shep.connector.EventParser;
import com.splunk.shep.connector.InvalidSignatureException;
import com.splunk.shep.connector.S2SDataHandler;
import com.splunk.shep.connector.S2SStateMachine;

public class StateMachineTest {
	S2SStateMachine stateMachine;
	public static void main(String[] args) throws Exception, InvalidSignatureException {
		if (args.length < 1) {
			System.out.println("Usage : java StateMachineTest <s2s-data-file>");
			return;
		}
		
		new StateMachineTest().testBasicStateMachine(args[0]);
		new StateMachineTest().testStateMachineWithFlumeEventParser(args[0]);
	}
	
	public void churnStateMachine(String fileName) throws Exception, InvalidSignatureException {
		FileInputStream fis = new FileInputStream(fileName);
		
		int read;
		byte[] buf = new byte[1000];
		while ((read = fis.read(buf)) != -1) {
			stateMachine.consume(buf, 0, read);
		}
		
		fis.close();
	}
	
	public void testBasicStateMachine(String fileName)  throws Exception, InvalidSignatureException {
		stateMachine = new S2SStateMachine(new S2SDataCallbackImpl());
		churnStateMachine(fileName);
	}

	public void testStateMachineWithFlumeEventParser(String fileName)  throws Exception, InvalidSignatureException {
		stateMachine = new S2SStateMachine(new TestFlumeDataHandler());
		churnStateMachine(fileName);
	}

	private class S2SDataCallbackImpl implements S2SDataHandler {
		private int count = 0;
		private Logger logger = Logger.getLogger(getClass());
		
		@Override
		public void s2sDataAvailable(byte[] raw) {
			logger.info("Got s2s data" + count++);
		}
		
	}

	private class TestFlumeDataHandler implements S2SDataHandler {
		private EventParser eventParser = new EventParser(null, 0);
		public TestFlumeDataHandler() {
		}
		
		/**
		 * Called where s2s state machine determines that it has decoded whole CowPipelineData
		 * @raw - s2s bytes
		 */
		public void s2sDataAvailable(byte[] raw) {
			eventParser.processS2SEvent(raw);
		}
	}
}

