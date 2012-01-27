// HDFSConnect.java
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.splunk.shep.connector.util.MetricsTimeout;
import com.splunk.shep.connector.util.TimeoutHeap;

/**
 * Needs a config file of the form
 * splunk2flume=<splunk_bind_ip>:<splunk_recv_port
 * >:<flume_ip>:<flume_port>;<splunk_bind_ip
 * >:<splunk_recv_port>:<flume_ip>:<flume_port>;... e.g.
 * splunk2flume=localhost:9997:flume_1:9991;localhost:9998:flume_2:9992
 */
public class HDFSConnect {
    protected List<BridgeConfig> configs = new ArrayList<BridgeConfig>();
    protected List<Acceptor> acceptors = new ArrayList<Acceptor>();
    public static final String OPT_HELP = "-help";
    private Logger logger = Logger.getLogger(getClass());

    public static void main(String[] args) throws Exception {
	HDFSConnect s2h = new HDFSConnect();
	if (args.length < 1) {
	    s2h.usage(args);
	    return;
	}

	if (!s2h.parseArgs(args))
	    return;
	s2h.parseConfig(args);
	s2h.createAcceptors();
	s2h.run();
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
	System.err.println("Usage : java " + getClass().getName()
		+ " <configfile>");
	System.err
		.println("configfile should have property splunk2hdfs or splunk2flume in the following form:\n"
			+ "directToHDFS=true/false"
			+ "splunk2hdfs=<splunk_bind_ip>:<splunk_recv_port>:<hadoop_ip>:<hadoop_port>:<file-name-path>;"
			+ "<splunk_bind_ip>:<splunk_recv_port>:<hadoop_ip>:<hadoop_port>:<file-name-path>;...\n"
			+ "splunk2flume=<splunk_bind_ip>:<splunk_recv_port>:<flume_ip>:<flume_port>;"
			+ "<splunk_bind_ip>:<splunk_recv_port>:<flume_ip>:<flume_port>;...");

	System.err.println("");

	Version.shortUsage(getClass().getName(), System.err);
	System.err.println("");
	Version.longUsage(System.err);
	System.err.println("");
	System.exit(-1);
    }

    protected void usage() {
	System.err.println("");
	System.err.println("Usage : java " + getClass().getName()
		+ " <configfile>");
	System.err
		.println("configfile should have property splunk2hdfs or splunk2flume in the following form:\n"
			+ "directToHDFS=true/false"
			+ "splunk2hdfs=<splunk_bind_ip>:<splunk_recv_port>:<hadoop_ip>:<hadoop_port>:<file-name-path>;"
			+ "<splunk_bind_ip>:<splunk_recv_port>:<hadoop_ip>:<hadoop_port>:<file-name-path>;...\n"
			+ "splunk2flume=<splunk_bind_ip>:<splunk_recv_port>:<flume_ip>:<flume_port>;"
			+ "<splunk_bind_ip>:<splunk_recv_port>:<flume_ip>:<flume_port>;...");

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
	    return false; // Already handled by Version, skip further processing
	}
	return true;
    }

    protected void parseConfig(String[] args) throws FileNotFoundException,
	    IOException {
	Properties prop = new Properties();
	prop.load(new FileInputStream(args[0]));

	boolean directHDFS = false;
	long maxEventSize = 32000;
	long fileRollSize = 10000000;
	boolean useAppend = false;

	String eventSize = prop.getProperty("maxEventSize_KB");
	String fileSize = prop.getProperty("fileRollingSize_MB");
	String usingHDFS = prop.getProperty("directToHDFS");
	String useAppending = prop.getProperty("useAppending");

	if (eventSize != null) {
	    maxEventSize = (long) Integer.parseInt(eventSize) * 1000;
	}

	if (fileSize != null) {
	    fileRollSize = (long) Integer.parseInt(fileSize) * 1000000;
	}

	if (usingHDFS != null) {
	    String flag = usingHDFS.toLowerCase();
	    if (flag.indexOf("true") >= 0)
		directHDFS = true; // default setting.
	}

	if (useAppending != null) {
	    String flag = useAppending.toLowerCase();
	    if (flag.indexOf("true") >= 0)
		useAppend = true; // default setting.
	}

	if (!directHDFS)
	    addFlumeConf(prop.getProperty("splunk2flume"), maxEventSize);

	else
	    addHDFSConf(prop.getProperty("splunk2hdfs"), fileRollSize,
		    useAppend);

	if (configs.size() <= 0) {
	    System.err
		    .println("HDFS Connector error: cofiguration not available.");
	    usage();
	    logger.error("cofiguration not available");
	    System.exit(-1);
	}
    }

    protected void addFlumeConf(String s2f, long maxEventSize)
	    throws FileNotFoundException, IOException {
	logger.info("Connect HDFS via Flume.");

	if (s2f != null) {
	    StringTokenizer st = new StringTokenizer(s2f, ";");
	    while (st.hasMoreElements()) {
		String token = st.nextToken();
		StringTokenizer portmapst = new StringTokenizer(token, ":");

		if (portmapst.countTokens() != 4) {
		    usage();
		}

		String bindip = portmapst.nextToken();
		int port = Integer.parseInt(portmapst.nextToken());
		String flumeip = portmapst.nextToken();
		int flumeport = Integer.parseInt(portmapst.nextToken());

		BridgeConfig conf = new BridgeConfig();
		conf.setHDFSMode(false);
		conf.setConnectionParams(bindip, port, flumeip, flumeport,
			maxEventSize);
		configs.add(conf);
	    }

	    System.out.println("Running Connection to Flume.");
	}
    }

    protected void addHDFSConf(String s2hdfs, long fileSize, boolean useAppend)
	    throws FileNotFoundException, IOException {
	logger.info("Connect HDFS directly.");

	if (s2hdfs != null) {
	    StringTokenizer st = new StringTokenizer(s2hdfs, ";");
	    while (st.hasMoreElements()) {
		String token = st.nextToken();
		StringTokenizer portmapst = new StringTokenizer(token, ":");

		if (portmapst.countTokens() != 5) {
		    usage();
		}

		String bindip = portmapst.nextToken();
		int bindport = Integer.parseInt(portmapst.nextToken());
		String tarip = portmapst.nextToken();
		int tarport = Integer.parseInt(portmapst.nextToken());
		String tarpath = portmapst.nextToken();

		BridgeConfig conf = new BridgeConfig();
		conf.setHDFSMode(true);
		conf.setConnectionParams(bindip, bindport, tarip, tarport,
			tarpath, fileSize);
		conf.setAppend(useAppend);
		configs.add(conf);
	    }

	    System.out.println("Running Connection to Hadoop directly.");
	}
    }

    protected void createAcceptors() {
	for (BridgeConfig conf : configs) {
	    acceptors.add(new S2SAcceptor(conf, new FlumeDataHandlerFactory(
		    conf)));
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

	System.out.println("To exit, create a file '.quit'");

	while (!quit()) {
	    try {
		Thread.sleep(1000);
	    } catch (InterruptedException e) {
		logger.error("Exception in sleep call: " + e.toString()
			+ "\nStacktrace:\n" + e.getStackTrace().toString());
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
