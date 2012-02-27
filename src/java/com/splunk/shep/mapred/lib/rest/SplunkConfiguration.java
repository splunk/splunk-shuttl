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

package com.splunk.shep.mapred.lib.rest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Convenience methods to configure for Splunk access
 * 
 * @author kpakkirisamy
 */
public class SplunkConfiguration {

    public static final String SPLUNKHOST = "splunk.host";
    public static final String SPLUNKINDEX = "splunk.index";
    public static final String SPLUNKSOURCETYPE = "splunk.sourcetype";
    public static final String INDEXBYHOST = "INDEXBYHOST";
    public final static String SPLUNKPORT = "splunk.port";
    public final static String USERNAME = "username";
    public final static String PASSWORD = "password";
    public final static String QUERY = "search";
    public final static String STARTTIME = "earliest_time";
    public final static String ENDTIME = "latest_time";
    public final static String TIMEFORMAT = "time_format";
    public final static String NUMSPLITS = "numsplits";
    public final static String SPLUNKEVENTREADER = "splunkeventreader";
    public final static String SPLUNK_SEARCH_URL = "/servicesNS/admin/search/search/jobs/export";
    public final static String INDEXHOST = "indexhost";

    public final static String SPLUNKDEFAULTSOURCETYPE = "hadoop_event";
    public final static String SPLUNKDEFAULTINDEX = "main";
    public final static String SPLUNKDEFAULTHOST = "localhost";
    public final static int SPLUNKDEFAULTPORT = 8089;

    private String host = SPLUNKDEFAULTHOST;
    private String sourceType = SPLUNKDEFAULTSOURCETYPE;
    private String splunkIndex = SPLUNKDEFAULTINDEX;
    private int mgmtPort = SPLUNKDEFAULTPORT;

    private static Logger logger = Logger.getLogger(SplunkConfiguration.class);

    public SplunkConfiguration(String confpath) throws FileNotFoundException,
	    IOException {
	parseConfig(confpath);
    }

    /**
     * Method to convenienty set various connection parameters
     * 
     * @param job
     *            Job Configuration
     * @param host
     *            Splunk Host name
     * @param port
     *            Splunk Management port
     * @param username
     *            Splunk User name
     * @param password
     *            Splunk password
     */
    public static void setConnInfo(JobConf job, String host, int port,
	    String username, String password) {
	job.set(SPLUNKHOST, host);
	job.setInt(SPLUNKPORT, port);
	job.set(USERNAME, username);
	job.set(PASSWORD, password);
	job.set(SPLUNKSOURCETYPE, SPLUNKDEFAULTSOURCETYPE);
	job.set(SPLUNKINDEX, SPLUNKDEFAULTINDEX);
    }

    public static void setConnInfo(JobConf job, String username, String password) {
	job.set(SPLUNKHOST, SPLUNKDEFAULTHOST);
	job.setInt(SPLUNKPORT, SPLUNKDEFAULTPORT);
	job.set(USERNAME, username);
	job.set(PASSWORD, password);
	job.set(SPLUNKSOURCETYPE, SPLUNKDEFAULTSOURCETYPE);
	job.set(SPLUNKINDEX, SPLUNKDEFAULTINDEX);
    }

    public static void setJobConf(JobConf job, String host, int port,
	    String sourcetype, String index, String username, String password) {
	job.set(SPLUNKHOST, host);
	job.setInt(SPLUNKPORT, port);
	job.set(USERNAME, username);
	job.set(PASSWORD, password);
	job.set(SPLUNKSOURCETYPE, sourcetype);
	job.set(SPLUNKINDEX, index);
    }

    public void setJobConf(JobConf job, String username, String password) {
	job.set(SPLUNKHOST, host);
	job.set(SPLUNKSOURCETYPE, sourceType);
	job.set(SPLUNKINDEX, splunkIndex);
	job.setInt(SPLUNKPORT, mgmtPort);
	job.set(USERNAME, username);
	job.set(PASSWORD, password);
    }

    /**
     * Method to specify the Splunk search query
     * 
     * @param job
     *            Job Configuration
     * @param query
     *            Splunk search query string
     * @param indexers
     *            Array of Splunk Indexer host names
     */
    public static void setSplunkQueryByIndexers(JobConf job, String query,
	    String indexers[]) {
	setSplunkQueryByIndexers(job, query, indexers, indexers.length);
    }

    private static void setSplunkQueryByIndexers(JobConf job, String query,
	    String indexers[], int numsplits) {
	logger.trace("query " + query + " num of splits " + indexers.length);
	job.set(QUERY, query);
	job.setInt(INDEXBYHOST, 1);
	job.setInt(NUMSPLITS, numsplits);
	for (int i = 0; i < indexers.length; i++) {
	    if (indexers.length > 0) {
		job.set(INDEXHOST + i, indexers[i]);
	    }
	}

	// distribute the splits across the indexers
	if (numsplits > indexers.length) {
	    for (int i = indexers.length; i < numsplits; i++) {
		job.set(INDEXHOST + i, indexers[i % indexers.length]);
	    }
	}
    }

    protected void parseConfig(String confpath) throws FileNotFoundException,
	    IOException {
	Properties prop = new Properties();
	prop.load(new FileInputStream(confpath));

	String hostname = prop.getProperty("host");
	String srctype = prop.getProperty("sourcetype");
	String index = prop.getProperty("index");
	String port = prop.getProperty("port");

	if (hostname != null) {
	    host = hostname;
	}

	if (srctype != null) {
	    sourceType = srctype;
	}

	if (index != null) {
	    splunkIndex = index;
	}

	if (port != null) {
	    mgmtPort = Integer.parseInt(port);
	}
    }

}
