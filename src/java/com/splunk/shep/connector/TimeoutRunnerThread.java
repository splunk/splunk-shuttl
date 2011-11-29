// TimeoutRunnerThread.java
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

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.splunk.shep.connector.util.Timeout;
import com.splunk.shep.connector.util.TimeoutHeap;

/**
 * This needs to be on its own thread. Potentially it could have been shared
 * with ConnectionManagerImpl, but since handlers can potentially block causing
 * Timeout to block.
 * 
 * @author jkerai
 * 
 */
public class TimeoutRunnerThread extends Thread {
    private Logger logger = Logger.getLogger(getClass());
    private Selector selector;
    private boolean stopThread = false;

    public void run() {
	try {
	    selector = Selector.open();
	    runTimeouts();
	} catch (IOException e) {
	    logger.error("TimeoutRunnerThread", e);
	}
    }

    public void runTimeouts() throws IOException {
	logger.debug("Waiting for timeout...");
	// Create a selector
	while (!shouldShutdown()) {
	    selector.select(100); // Will block for 100ms.

	    // Process each timeout
	    List<Timeout> rescheduledTimeouts = new ArrayList<Timeout>();
	    Date now = new Date();
	    Timeout t;
	    while ((t = TimeoutHeap.getExpiredTimeout(now)) != null) {
		try {
		    if (!t.run()) {
			logger.debug("Removing timeout");
			TimeoutHeap.removeTimeout(t);
		    } else {
			rescheduledTimeouts.add(t);
		    }
		} catch (Exception e) {
		    logger.error("Timeout exception", e);
		    e.printStackTrace();
		    TimeoutHeap.removeTimeout(t);
		}
	    }
	    for (Timeout to : rescheduledTimeouts) {
		TimeoutHeap.addTimeout(to);
	    }
	}
    }

    private boolean shouldShutdown() {
	synchronized (this) {
	    return stopThread;
	}
    }

    public void shutdown() {
	synchronized (this) {
	    stopThread = true;
	    selector.wakeup();
	}
	try {
	    logger.info("Waiting for timeoutRunner thread to stop");
	    join();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}

    }
}
