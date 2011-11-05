// Splunk2Flume.java
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

package com.splunk.connector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;

import com.splunk.connector.util.MetricsTimeout;
import com.splunk.connector.util.TimeoutHeap;

/**
 * Needs a config file of the form
 * splunk2flume=<splunk_bind_ip>:<splunk_recv_port>:<flume_ip>:<flume_port>;<splunk_bind_ip>:<splunk_recv_port>:<flume_ip>:<flume_port>;...
 * e.g.
 * splunk2flume=localhost:9997:flume_1:9991;localhost:9998:flume_2:9992
 */
public class Splunk2Flume 
{
	protected List<BridgeConfig> configs = new ArrayList<BridgeConfig>();
	protected List<Acceptor> acceptors = new ArrayList<Acceptor>();
	public static final String OPT_HELP = "-help";
	private Logger logger = Logger.getLogger(getClass());
	
	public static void main(String[] args) throws Exception {
		Splunk2Flume s2f = new Splunk2Flume();
		if (args.length < 1) {
			s2f.usage(args);
			return;
		}
		
		if (!s2f.parseArgs(args))
			return;
		s2f.parseConfig(args);
		s2f.createAcceptors();
		s2f.run();
	}

	public void start(String filename) throws Exception {
		String args[] = new String[1];
		args[0] = filename;
		parseConfig(args);
		createAcceptors();
		run();
	}
	
	protected void usage(String[] args) {
		System.err.println("");
		System.err.println("Usage : java " + getClass().getName() + " <configfile>");
		System.err.println("configfile should have property splunk2flume in the following form:\n" +
				"splunk2flume=<splunk_bind_ip>:<splunk_recv_port>:<flume_ip>:<flume_port>;" + 
				"<splunk_bind_ip>:<splunk_recv_port>:<flume_ip>:<flume_port>;...");

		System.err.println("");
		
		Version.shortUsage(getClass().getName(), System.err);
		System.err.println("");
		Version.longUsage(System.err);
		System.err.println("");
		System.exit(-1);
	}
	
	protected boolean parseArgs(String[] args) {
		if (args.length < 1) {
			usage(args);
			return false;
		}
		
		// Explicit check for help
		if (args[0].equals(OPT_HELP)) {
			usage(args);
			return false;
		}
		
		if (Version.handleCli(getClass().getName(), args, System.out)) {
			return false;	// Already handled by Version, skip further processing
		}
		return true;
	}
	
	protected void parseConfig(String[] args) throws FileNotFoundException, IOException {
		Properties prop = new Properties();
		prop.load(new FileInputStream(args[0]));
		
		long maxEventSize = 32000;
		String eventSize = prop.getProperty("maxEventSize_KB");
		if (eventSize != null)
		{
			maxEventSize = (long) Integer.parseInt(eventSize) * 1000;
		}
		
		String s2f = prop.getProperty("splunk2flume");
		if (s2f == null) {
			usage(args);
		}
		
		StringTokenizer st = new StringTokenizer(s2f, ";");
		while (st.hasMoreElements()) {
			String token = st.nextToken();
			StringTokenizer portmapst = new StringTokenizer(token, ":");
			if (portmapst.countTokens() != 4) {
				usage(args);
			}

			String bindip = portmapst.nextToken();
			int port = Integer.parseInt(portmapst.nextToken());
			String flumeip = portmapst.nextToken();
			int flumeport = Integer.parseInt(portmapst.nextToken());
			
			configs.add(new BridgeConfig(bindip, port, flumeip, flumeport, maxEventSize));
		}
	}
	
	protected void createAcceptors() {
		for (BridgeConfig conf : configs) {
			acceptors.add(new S2SAcceptor(conf, new FlumeDataHandlerFactory(conf)));
		}
	}

	protected void run() throws FileNotFoundException, IOException {
		ConnectionManager cm = new ConnectionManager();
		cm.listen(acceptors);
		cm.run();
		
		// Create singleton instance
		EventThruput.getInstance();
		
		MetricsTimeout metricsTimeout = new MetricsTimeout(30 * 1000);
		TimeoutHeap.addTimeout(metricsTimeout);
		TimeoutRunnerThread timeoutRunnerThread = new TimeoutRunnerThread();
		timeoutRunnerThread.start();
		
		System.out.println("To exit create a file '.quit'");
		
		while (!quit()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		cm.shutdown();
		timeoutRunnerThread.shutdown();
	}
	
	// Check to see .quit file is present
	private boolean quit() {
		String curDir = System.getProperty("user.dir");
		File quitFile = new File(curDir, ".quit");
		if (quitFile.exists()) {
			logger.warn("Found .quit file. Will exit");
			return true;
		}
		return false;
	}
}
