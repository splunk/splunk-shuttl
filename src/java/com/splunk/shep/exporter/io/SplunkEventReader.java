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
package com.splunk.shep.exporter.io;

import static com.splunk.shep.ShepConstants.OutputMode.*;
import static com.splunk.shep.exporter.SplunkEventFormatter.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.splunk.Args;
import com.splunk.Service;
import com.splunk.shep.ShepConstants.OutputMode;
import com.splunk.shep.exporter.SplunkEventFormatter;
import com.splunk.shep.exporter.model.ShepExport;
import com.splunk.shep.exporter.model.SplunkEvent;
import com.splunk.shep.mapreduce.lib.rest.SplunkXMLStream;

/**
 * @author hyan
 *
 */
public class SplunkEventReader extends EventReader {
    private static Logger log = Logger.getLogger(SplunkEventReader.class);
    private Service service;
    public static final long TIME_GAP = 5;
    // private InputStream input;
    private SplunkXMLStream parser;

    public SplunkEventReader() {
	// TODO get splunk info from jetty config file, default.properties is
	// for testing only?
	String host = "localhost";
	int port = 8089;
	String username = "admin";
	String password = "changeme";
	Args args = new Args();
	// TODO currently splunk-java-sdk hardcode these key names in its
	// Service class. When they refactor their code to define them as public
	// constants, we will get the key names from sdk.
	args.add("username", username);
	args.add("password", password);
	args.add("host", host);
	args.add("port", port);
	service = Service.connect(args);
    }

    @Override
    public InputStream export(String indexName, long lastEndTime,
	    OutputMode outputMode) throws IOException {

	String disjunction = disjunction(lastEndTime + 1);

	String query = String.format("search index=\"%s\" %s", indexName,
		disjunction);
	log.debug(String.format("lastEndTime: %d, endTime: %d", lastEndTime,
		endTime));
	log.debug("query: " + query);

	Args args = new Args();
	args.add("output_mode", outputMode.toString());
	return service.export(query, args);
    }

    @Override
    public String nextEvent(String indexName, long lastEndTime, OutputMode outputMode) throws IOException {
	String event = null;
	try {
	    if (parser == null) {
		InputStream input = export(indexName, lastEndTime, xml);
		parser = new SplunkXMLStream(input);
	    }

	    Map<String, String> map = parser.nextResult();
	    log.debug("map: " + map);
	    if (map != null) {
		event = SplunkEventFormatter.mapToString(map, outputMode);
	    }
	} catch (Exception e) {
	    log.error(
		    "got exception while parsing splunk event from InputStream",
		    e);
	    throw new IOException("Failed to get next event: " + e.getMessage());
	}
	log.debug("event: " + event);
	return event;

    }

    @Override
    public String nextEvents(String indexName, long lastEndTime,
	    OutputMode outputMode, int numEvents) throws IOException {
	String result = null;
	try {
	    if (parser == null) {
		InputStream input = export(indexName, lastEndTime, xml);
		parser = new SplunkXMLStream(input);
	    }

	    ShepExport shepExport = new ShepExport();
	    List<SplunkEvent> splunkEvents = shepExport.getSplunkEvent();
	    int i = 0;
	    String nextEvent = null;
	    Map<String, String> map;
	    do {
		map = parser.nextResult();
		log.debug("map: " + map);
		if (map == null) {
		    break;
		}
		SplunkEvent splunkEvent = SplunkEventFormatter.mapToObject(map);
		splunkEvents.add(splunkEvent);
		i++;
	    } while (i < numEvents && map != null);
	    // actual = objectToJson(shepExport, ShepExport.class);
	    // FIXME move this utility class out of testUtil class
	    if (outputMode == xml) {
		result = objectToString(shepExport, ShepExport.class,
			outputMode);

	    } else if (outputMode == json) {
		result = objectToString(splunkEvents, splunkEvents.getClass(),
			outputMode);
	    }
	} catch (Exception e) {
	    log.error(
		    "got exception while parsing splunk event from InputStream",
		    e);
	    throw new IOException("Failed to get next event: " + e.getMessage());
	}
	log.debug("result: " + result);
	return result;

    }

    String disjunction(long lastEndTime) {
	endTime = System.currentTimeMillis() / 1000 - TIME_GAP;
	StringBuilder disjuncts = new StringBuilder();
	int level;
	boolean first = true;

	while (lastEndTime < endTime) {
	    level = 10;
	    while ((lastEndTime % level == 0)
		    && (level * (lastEndTime / level) + level < endTime)) {
		level = level * 10;
	    }
	    level = level / 10;
	    if (first) {
		disjuncts.append(" AND (");
	    } else {
		disjuncts.append(" OR ");
	    }
	    disjuncts.append("_indextime = ").append(lastEndTime / level)
		    .append((level > 1) ? "*" : "");
	    if (first) {
		first = false;
	    }
	    lastEndTime = level * lastEndTime / level + level;
	}
	if (disjuncts.length() > 0) {
	    disjuncts.append(")");
	}

	return disjuncts.toString();
    }

}
