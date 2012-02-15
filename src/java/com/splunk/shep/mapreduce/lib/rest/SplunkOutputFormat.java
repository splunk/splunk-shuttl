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

import java.io.*;
import java.net.Socket;
import java.util.Date;

import com.splunk.Args;
import com.splunk.Index;
import com.splunk.Service;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;


/**
 * An OutputFormat that send reduce output to Splunk as a simple event with
 * space separated key and value, prefixed with a time stamp on when this was
 * generated
 *
 * @param <K>
 * @param <V>
 * @author kpakkirisamy
 */

public class SplunkOutputFormat<K, V> implements OutputFormat<K, V> {
    private static final String HADOOP_EVENT = "hadoop_event";
    public final static String SPLUNKHOST = SplunkConfiguration.SPLUNKHOST;
    public final static String SPLUNKPORT = SplunkConfiguration.SPLUNKPORT;
    public final static String USERNAME = SplunkConfiguration.USERNAME;
    public final static String PASSWORD = SplunkConfiguration.PASSWORD;
    private static Logger logger = Logger
            .getLogger(SplunkOutputFormat.class);
    private Service service = null;
    private Socket stream = null;
    private Writer writerOut;

    /**
     * A RecordWriter that writes the reduce output to Splunk
     */
    protected class SplunkRecordWriter implements RecordWriter<K, V> {
        private static final String SPACE = " ";

        protected SplunkRecordWriter() {
            System.out.println("SplunkRecordWriter Constructor!!!");
        }

        public void close(Reporter reporter) throws IOException {
        }

        public void write(K key, V value) throws IOException {
            logger.trace("key " + key + " value " + value);

            StringBuilder sbuf = new StringBuilder();
            sbuf.append(new Date().toString());
            sbuf.append(SPACE);             // space separator for Splunk
            sbuf.append(key.toString());
            sbuf.append(SPACE);             // space separator for Splunk
            sbuf.append(value.toString());
            sbuf.append("\n");
            String eventString = sbuf.toString();
            writerOut.write(eventString);
            writerOut.flush();
        }
    }

    private void loginSplunk(JobConf job) {
        try {
            if (service == null) {
                //build up login
                Args args = new Args();
                args.put("username", job.get(SplunkConfiguration.USERNAME));
                args.put("password", job.get(SplunkConfiguration.PASSWORD));
                args.put("host", job.get(SplunkConfiguration.SPLUNKHOST));
                args.put("port", job.getInt(SplunkConfiguration.SPLUNKPORT, 8089));
                service = Service.connect(args);
            }
            if (stream == null) {
                // create a an http stream input assume "main" index.
                // wkcifx: add allowance for different index through
                // hadoop job settings (like user/pass/etc).

                Index index = service.getIndexes().get("main");
                Args attachArgs = new Args();
                attachArgs.put("source", job.getJobName());
                attachArgs.put("sourcetype", HADOOP_EVENT);
                stream = index.attach(attachArgs);
                OutputStream ostream = stream.getOutputStream();
                writerOut = new OutputStreamWriter(ostream, "UTF8");
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to connect to splunk, "
                                     + "or connect to streaming socket");
        }
    }

    /**
     * Get the {@link RecordWriter} for the given job.
     *
     * @param ignored  ignored param
     * @param job      configuration for the job whose output is being written.
     * @param name     the unique name for this part of the output.
     * @param progress mechanism for reporting progress while writing to file.
     * @return a {@link RecordWriter} to write the output for the job.
     * @throws IOException
     */
    public RecordWriter<K, V> getRecordWriter(
        FileSystem ignored, JobConf job,String name, Progressable progress)
        throws IOException {

        loginSplunk(job);

        return new SplunkRecordWriter();
    }

    /**
     * Check for validity of the output-specification for the job.
     * <p/>
     * <p>
     * This is to validate the output specification for the job when it is a job
     * is submitted. Typically checks that it does not already exist, throwing
     * an exception when it already exists, so that output is not overwritten.
     * </p>
     *
     * @param ignored ignored param
     * @param job     job configuration.
     * @throws IOException when output should not be attempted
     */
    public void checkOutputSpecs(FileSystem ignored, JobConf job)
            throws IOException {
    }
}
