// EventThruput.java
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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.splunk.shep.connector.util.MetricsCallback;
import com.splunk.shep.connector.util.MetricsManager;

public class S2SEventThruput implements MetricsCallback {
    private class Thruput {
	private long totalBytes = 0;
	private int eventCount = 0;

	public void update(long size) {
	    totalBytes += size;
	    eventCount++;
	}
    }

    private Map<String, Thruput> perHostThruput = new HashMap<String, Thruput>();
    private String lastHost;
    private Thruput lastHostThruput;

    private Map<String, Thruput> perSourceThruput = new HashMap<String, Thruput>();
    private String lastSource;
    private Thruput lastSourceThruput;

    private Map<String, Thruput> perSourcetypeThruput = new HashMap<String, Thruput>();
    private String lastSourcetype;
    private Thruput lastSourcetypeThruput;

    private Map<String, Thruput> perIndexThruput = new HashMap<String, Thruput>();
    private String lastIndex;
    private Thruput lastIndexThruput;

    private long bytesSeenSinceLastOutput = 0;
    private int eventsSeenSinceLastOutput = 0;
    private double totalBytesSent = 0;
    private Date lastOutputTime = new Date();
    private Date startTime = new Date();
    private static S2SEventThruput instance = null;

    public static S2SEventThruput getInstance() {
	synchronized (S2SEventThruput.class) {
	    if (instance == null)
		instance = new S2SEventThruput();
	}
	return instance;
    }

    public synchronized void update(String host, String source,
	    String sourcetype, String index, long size) {
	lastHostThruput = updateThruput(host, lastHost, perHostThruput,
		lastHostThruput, size);
	lastHost = host;

	lastSourceThruput = updateThruput(source, lastSource, perSourceThruput,
		lastSourceThruput, size);
	lastSource = source;

	lastSourcetypeThruput = updateThruput(sourcetype, lastSourcetype,
		perSourcetypeThruput, lastSourcetypeThruput, size);
	lastSourcetype = sourcetype;

	lastIndexThruput = updateThruput(index, lastIndex, perIndexThruput,
		lastIndexThruput, size);
	lastIndex = index;

	bytesSeenSinceLastOutput += size;
	totalBytesSent += size;
	eventsSeenSinceLastOutput++;
    }

    private S2SEventThruput() {
	MetricsManager.getInstance().register(this);
    }

    public void close() {
	MetricsManager.getInstance().unregister(this);
    }

    private Thruput updateThruput(String newKey, String prevKey,
	    Map<String, Thruput> thruputMap, Thruput lastThruput, long size) {
	Thruput t;
	if ((newKey != prevKey) || (prevKey == null)) {
	    if (thruputMap.containsKey(newKey)) {
		t = thruputMap.get(newKey);
	    } else {
		t = new Thruput();
		thruputMap.put(newKey, t);
	    }
	} else {
	    t = lastThruput;
	}
	t.update(size);
	return t;
    }

    @Override
    public synchronized void generateMetrics(Logger logger) {
	Date now = new Date();
	long timeDiff = (now.getTime() - lastOutputTime.getTime()) / 1000;
	if (timeDiff <= 0)
	    return;

	double kb = ((double) bytesSeenSinceLastOutput / 1024);
	double kbps = kb / timeDiff;
	double eps = (double) (eventsSeenSinceLastOutput / timeDiff);
	double avgKbps = ((double) totalBytesSent / 1024)
		/ ((now.getTime() - startTime.getTime()) * 1000);

	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	PrintStream ps = new PrintStream(baos);
	ps.print(" Metrics - group=thruput name=connector_thruput");
	ps.format(" instantaneous_kbps=%.4f", kbps);
	ps.format(" instantaneous_eps=%.4f", eps);
	ps.format(" average_kbps=%.4f", avgKbps);
	ps.format(" total_k_processed=%.4f", ((double) totalBytesSent / 1024));
	ps.format(" kb=%.4f", kb);
	ps.print(" ev=" + eventsSeenSinceLastOutput);
	logger.info(baos.toString());
	baos.reset();

	generateMetrics(logger, "per_host_thruput", perHostThruput, timeDiff);
	generateMetrics(logger, "per_source_thruput", perSourceThruput,
		timeDiff);
	generateMetrics(logger, "per_sourcetype_thruput", perSourcetypeThruput,
		timeDiff);
	generateMetrics(logger, "per_index_thruput", perIndexThruput, timeDiff);

	// Make sure to clear these metrics
	perHostThruput.clear();
	perSourceThruput.clear();
	perSourcetypeThruput.clear();
	perIndexThruput.clear();

	bytesSeenSinceLastOutput = 0;
	eventsSeenSinceLastOutput = 0;

	lastOutputTime = new Date();
    }

    private void generateMetrics(Logger logger, String group,
	    Map<String, Thruput> thruput, long timeDiff) {
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	PrintStream ps = new PrintStream(baos);
	for (String series : thruput.keySet()) {
	    double kb = (double) thruput.get(series).totalBytes / 1024;
	    double kbps = kb / timeDiff;
	    int ev = thruput.get(series).eventCount;
	    double eps = (double) (ev / timeDiff);
	    ps.print(" Metrics -");
	    ps.print(" group=" + group);
	    ps.print(" series=" + series);
	    ps.format(" kbps=%.4f", kbps);
	    ps.format(" eps=%.4f", eps);
	    ps.format(" kb=%.4f", kb);
	    ps.print(" ev=" + ev);
	    logger.info(baos.toString());
	    baos.reset();
	}
	thruput.clear();
    }
}
