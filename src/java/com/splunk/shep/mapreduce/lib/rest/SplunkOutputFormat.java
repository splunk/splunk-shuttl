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

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.log4j.Logger;

import com.splunk.shep.mapreduce.lib.rest.util.HttpClientUtils;

/**
 * An OutputFormat that send reduce output to Splunk as a simple event with
 * space separated key and value, prefixed with a time stamp on when this was
 * generated
 * 
 * @author kpakkirisamy
 * 
 * @param <K>
 * @param <V>
 */
public class SplunkOutputFormat<K, V> implements OutputFormat<K, V> {
    private static final String SPLUNK_SMPLRCVR_ENDPT = "/services/receivers/simple";
    private static final int SPLUNK_MGMTPORT_DEFAULT = 8089;
    private static final String HADOOP_EVENT = "hadoop_event"; // event can be
							       // configured on
							       // the Splunk
							       // side
    public final static String SPLUNKHOST = SplunkConfiguration.SPLUNKHOST;
    public final static String SPLUNKPORT = SplunkConfiguration.SPLUNKPORT;
    public final static String USERNAME = SplunkConfiguration.USERNAME;
    public final static String PASSWORD = SplunkConfiguration.PASSWORD;
    private final static int HTTP_OK = 200;
    private static Logger logger = logger = Logger
	    .getLogger(SplunkOutputFormat.class);

    /**
     * A RecordWriter that writes the reduce output to Splunk
     */
    protected class SplunkRecordWriter implements RecordWriter<K, V> {
	private static final String SPACE = " ";
	HttpClient httpclient;
	String poststring;

	protected SplunkRecordWriter(HttpClient httpclient, String poststr) {
	    this.httpclient = httpclient;
	    this.poststring = poststr;
	}

	public void close(Reporter reporter) throws IOException {
	}

	public void write(K key, V value) throws IOException {
	    logger.trace("key " + key + " value " + value);
	    HttpPost httppost = new HttpPost(this.poststring);
	    StringBuilder sbuf = new StringBuilder();
	    /**
	     * sbuf.append(URLEncoder.encode(new Date().toString()));
	     * sbuf.append(URLEncoder.encode(SPACE));
	     * sbuf.append(URLEncoder.encode(key.toString())); // space
	     * separated fields for Splunk to regex out
	     * sbuf.append(URLEncoder.encode(SPACE));
	     * sbuf.append(URLEncoder.encode(value.toString())); // space
	     * separated fields for Splunk to regex out
	     **/
	    sbuf.append(new Date().toString());
	    sbuf.append(SPACE);
	    sbuf.append(key.toString()); // space separated fields for Splunk to
					 // regex out
	    sbuf.append(SPACE);
	    sbuf.append(value.toString()); // space separated fields for Splunk
					   // to regex
					   // out
	    sbuf.append("\n");
	    StringEntity reqEntity = new StringEntity(sbuf.toString());
	    httppost.setEntity(reqEntity);
	    HttpResponse response = this.httpclient.execute(httppost);
	    if (response.getStatusLine().getStatusCode() != HTTP_OK) {
		logger.trace(response.getStatusLine());
		throw new IOException(response.getStatusLine().toString());
	    }
	    HttpEntity resEntity = response.getEntity();
	    resEntity.consumeContent();
	}
    }

    /**
     * Get the {@link RecordWriter} for the given job.
     * 
     * @param ignored
     * @param job
     *            configuration for the job whose output is being written.
     * @param name
     *            the unique name for this part of the output.
     * @param progress
     *            mechanism for reporting progress while writing to file.
     * @return a {@link RecordWriter} to write the output for the job.
     * @throws IOException
     */
    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
	    String name, Progressable progress) throws IOException {
	try {
	    HttpClient httpclient = HttpClientUtils.getHttpClient();
	    ((AbstractHttpClient) httpclient).getCredentialsProvider()
		    .setCredentials(
			    new AuthScope(job.get(SPLUNKHOST), job.getInt(
				    SPLUNKPORT, SPLUNK_MGMTPORT_DEFAULT)),
			    new UsernamePasswordCredentials(job.get(USERNAME),
				    job.get(PASSWORD)));
	    String poststr = "https://"
		    + job.get(SPLUNKHOST)
		    + ":"
		    + new Integer(job.getInt(SPLUNKPORT,
			    SPLUNK_MGMTPORT_DEFAULT)) + SPLUNK_SMPLRCVR_ENDPT
		    + "?source=" + job.getJobName() + "&sourcetype="
		    + HADOOP_EVENT;
	    logger.trace("POSTSTR: " + poststr);
	    return new SplunkRecordWriter(httpclient, poststr);
	} catch (Exception e) {
	    logger.trace(e);
	    throw new IOException(e);
	}
    }

    /**
     * Check for validity of the output-specification for the job.
     * 
     * <p>
     * This is to validate the output specification for the job when it is a job
     * is submitted. Typically checks that it does not already exist, throwing
     * an exception when it already exists, so that output is not overwritten.
     * </p>
     * 
     * @param ignored
     * @param job
     *            job configuration.
     * @throws IOException
     *             when output should not be attempted
     */
    public void checkOutputSpecs(FileSystem ignored, JobConf job)
	    throws IOException {
    }
}
