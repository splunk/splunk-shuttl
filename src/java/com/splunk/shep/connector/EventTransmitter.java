// EventExtractor.java
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

import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class EventTransmitter {
    private static Logger logger = Logger.getLogger(EventTransmitter.class);

    public static void main(String[] args) throws Exception {
	int recvPort = 9997;
	int targetPort = 8888;
	String targetIP = new String("0.0.0.0");
	// EventEmitter emitter = null;
	EventExtractor extractor = null;

	if (args.length > 0) {
	    recvPort = Integer.parseInt(args[0]);
	    if (recvPort <= 0)
		recvPort = 9997;

	    if (args.length > 1)
		targetPort = Integer.parseInt(args[1]);
	    if (targetPort <= 0)
		targetPort = 8888;

	    if (args.length > 2)
		targetIP = args[2];

	    if (args.length > 3)
		PropertyConfigurator.configure(args[3]);
	} else {
	    System.err
		    .println("Usage: EventTransmitter <recv-port> <target-port> [<target-ip>] [<properties-file>]");
	    return;
	}

	logger.info("started at port " + recvPort + " with tartget " + targetIP
		+ ":" + targetPort);

	while (true) {
	    try {
		// emitter = new EventEmitter(targetPort, targetIP);
		// extractor = new EventExtractor(recvPort, emitter);
		extractor = new EventExtractor(recvPort, targetPort, targetIP);

		// turn on debug flag.
		extractor.debug();

		// launch application
		// emitter.start();
		extractor.run();
	    } catch (Exception ex) {
		ex.printStackTrace();
		// emitter.close();
		extractor.closeAll();

		Date time = new Date();
		// logger.info((DateFormat.getInstance().format(time));
		logger.info("Restarting EventTransmitter at port " + recvPort
			+ " with tartget " + targetIP + ":" + targetPort);
	    }
	}
    }
}
