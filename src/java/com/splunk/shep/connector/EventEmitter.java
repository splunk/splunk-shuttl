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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.avro.AvroEventSink;

public class EventEmitter implements DataSink {

    private String ip = new String("0.0.0.0");
    private int port = 8888;
    private Socket echoSocket = null;
    private AvroEventSink snk = null;
    private long maxEventSize = 32000;

    private static Logger logger = Logger.getLogger(EventEmitter.class);

    public EventEmitter(int targetPort, String targetIP) {
	port = targetPort;
	ip = targetIP;
    }

    public void debug() {
    }

    public void setMaxEventSize(long size) {
	maxEventSize = size;
	logger.info("Max size of event: " + maxEventSize);
    }

    public void setFileRollingSize(long size) {
	// file size configuration is not supported with flume connection.
    }

    public int getPort() {
	return port;
    }

    public String getIp() {
	return ip;
    }

    public void run() throws Exception {
	logger.info("EventEmitter started with target " + ip + ":" + port);

	try {
	    start();
	} catch (Exception e) {
	    logger.error("Cannot start event collector, quit.");
	    return;
	}

	final BufferedReader stdIn = new BufferedReader(new InputStreamReader(
		System.in, "US-ASCII"));
	String userInput;

	while ((userInput = stdIn.readLine()) != null) {
	    if (!userInput.equals("event_begin{"))
		continue;

	    userInput = stdIn.readLine();
	    int rawlen = Integer.parseInt(userInput);
	    char[] raw_char = new char[rawlen];
	    int readLen = 0;
	    do {
		readLen += stdIn.read(raw_char, readLen, (rawlen - readLen));
	    } while (readLen < rawlen);

	    stdIn.readLine();
	    String raw_str = new String(raw_char);
	    byte[] raw_bytes = raw_str.getBytes("US-ASCII");

	    String host = stdIn.readLine();
	    int idx = host.indexOf("::");
	    if (idx >= 0) {
		host = host.substring(idx + 2);
	    }

	    String sourcetype = stdIn.readLine();
	    idx = sourcetype.indexOf("::");
	    if (idx >= 0) {
		sourcetype = sourcetype.substring(idx + 2);
	    }

	    String source = stdIn.readLine();
	    idx = source.indexOf("::");
	    if (idx >= 0) {
		source = source.substring(idx + 2);
	    }
	    source = source.replaceAll(":", "_");
	    source = source.replaceAll("\\s+", "_");
	    source = source.replaceAll("\\\\", "_");
	    source = source.replaceAll("/", "_");

	    String time = stdIn.readLine();
	    long unixtime = 0;
	    try {
		unixtime = Long.parseLong(time);
		unixtime *= 1000L;
	    } catch (java.lang.NumberFormatException ex) {
		logger.error("Exception in prasing timestamp: " + ex.toString()
			+ "\nStacktrace:\n" + ex.getStackTrace().toString());
	    }

	    stdIn.readLine();
	    logger.info("Processing event from host=" + host + ", source="
		    + source + ", sourcetype=" + sourcetype + ", timestamp="
		    + unixtime);
	    send(raw_bytes, sourcetype, source, host, unixtime);
	}

	logger.info("Closing input and output connections...");
	stdIn.close();
	snk.close();

	logger.info("EventEmitter quit.");
    }

    public void start() throws Exception {
	snk = new AvroEventSink(ip, port);
	snk.open();
	logger.info("started with target " + ip + ":" + port);
    }

    public void start(String sinkPath) throws Exception {
	start();
    }

    public void close() {
	try {
	    snk.close();
	} catch (Exception ex) {
	    ex.printStackTrace();
	}

	logger.info("closed.");

    }

    public void send(String data, String sourceType, String source,
	    String host, long time) throws Exception {
	send(data.getBytes(), sourceType, source, host, time);
    }

    public void send(byte[] rawBytes) throws Exception {
	send(rawBytes, "Unknown", "Unknown", "0", System.currentTimeMillis());
    }

    public void send(byte[] rawBytes, String sourceType, String source,
	    String host, long time) throws Exception {
	String msg = new String(rawBytes);
	HashMap<String, byte[]> map = new HashMap<String, byte[]>();
	map.put("sourceType", sourceType.getBytes());
	map.put("source", source.getBytes());
	logger.debug("Sending event_host=" + host + ", event_source=" + source
		+ ", event_sourcetype=" + sourceType + ", event_time=" + time
		+ ", event_rawlen=" + rawBytes.length + ", event_data=" + msg);

	EventImpl ei = null;

	try {
	    ei = new EventImpl(rawBytes, time, Priority.INFO, 0, host, map);
	} catch (Exception ex) {
	    logger.error("cannot build event object. Skip msg: host=" + host
		    + ", source=" + source + ", sourcetype=" + sourceType
		    + ", event_time=" + time + ", rawBytes=" + rawBytes.length
		    + ", data: " + msg + "\nStack trace:\n"
		    + ex.getStackTrace().toString());
	    return;
	}

	while (true) {
	    try {
		if (rawBytes.length < maxEventSize)
		    snk.append(ei);
		else
		    logger.warn("event too long to send. Skip msg: host="
			    + host + ", source=" + source + ", sourcetype="
			    + sourceType + ", event_time=" + time
			    + ", rawBytes=" + rawBytes.length + ", data: "
			    + msg);
		break;
	    } catch (java.lang.Exception ex) {
		logger.warn("exception in sending data to Flume, try reconnection.\nStacktrace:\n"
			+ ex.getStackTrace().toString());

		try {
		    Thread.currentThread().sleep(1000); // sleep for 1000 ms.
		    snk.close();
		    logger.debug("Closed connection to Flume.");
		} catch (Exception e) {
		    logger.warn("exception in closing connection to Flume.\nStacktrace:\n"
			    + e.getStackTrace().toString());
		}

		try {
		    Thread.currentThread().sleep(1000); // sleep for 1000 ms.
		    snk.open();
		    logger.debug("Reopened connection to Flume.");
		} catch (Exception e) {
		    logger.warn("exception in opening connection to Flume.\nStacktrace:\n"
			    + e.getStackTrace().toString());
		}
	    }
	}
	logger.debug("Sent 1 event to Flume.");
    }

    public static void main(String[] args) throws IOException {
	int targetPort = 8888;
	String targetIP = new String("0.0.0.0");

	if (args.length > 0) {
	    targetPort = Integer.parseInt(args[0]);
	    if (args.length > 1)
		targetIP = args[1];
	    if (args.length > 2)
		PropertyConfigurator.configure(args[2]);
	} else {
	    System.out
		    .println("Please start with port (required) and ip (optional).\nUsage: EventEmitter <port-number> [<ip-address>] [<properties-file>]");
	    return;
	}

	EventEmitter emitter = null;

	try {
	    emitter = new EventEmitter(targetPort, targetIP);
	    emitter.debug();
	    emitter.run();
	} catch (Exception ex) {
	    ex.printStackTrace();
	} finally {
	    emitter.close();
	}
    }
}
