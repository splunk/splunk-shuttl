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

package com.splunk.shep.mapreduce.lib.rest;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Convenience methods to configure for Splunk access
 * 
 * @author kpakkirisamy
 */
public class SplunkConfiguration {

    // hadoop job keys
    public static final String INDEXBYHOST = "INDEXBYHOST";
    public final static String SPLUNKHOST = "splunk.host";
    public final static String SPLUNKPORT = "splunk.port";
    public final static String USERNAME = "username";
    public final static String PASSWORD = "password";
    public final static String QUERY = "search";
    public final static String STARTTIME = "earliest_time";
    public final static String ENDTIME = "latest_time";
    public final static String TIMEFORMAT = "time_format";
    public final static String NUMSPLITS = "numsplits";
    public final static String SPLUNKEVENTREADER = "splunkeventreader";
    public final static String INDEXHOST = "indexhost";

    private static Logger logger = Logger.getLogger(SplunkConfiguration.class);

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
}
