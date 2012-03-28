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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import com.splunk.Args;
import com.splunk.Index;
import com.splunk.Service;

/**
 * An OutputFormat that send reduce output to Splunk as a simple event with
 * space separated key and value, prefixed with a time stamp on when this was
 * generated
 * 
 * @param <K>
 * @param <V>
 * @author kpakkirisamy, hyan
 */

public class SplunkOutputFormat<K, V> extends OutputFormat<K, V> {
    public final static String SPLUNKHOST = SplunkConfiguration.SPLUNKHOST;
    public final static String SPLUNKPORT = SplunkConfiguration.SPLUNKPORT;
    public final static String USERNAME = SplunkConfiguration.USERNAME;
    public final static String PASSWORD = SplunkConfiguration.PASSWORD;

    private static Logger LOG = Logger.getLogger(SplunkOutputFormat.class);
    private FileOutputCommitter committer = null;
    private Service service = null;
    private Socket stream = null;
    private Writer writerOut;

    /**
     * A RecordWriter that writes the reduce output to Splunk
     */
    protected class SplunkRecordWriter extends RecordWriter<K, V> {
	private static final String SPACE = " ";

	protected SplunkRecordWriter() {

	}

	@Override
	public void write(K key, V value) throws IOException {
	    StringBuilder sbuf = new StringBuilder();
	    sbuf.append(new Date().toString());
	    sbuf.append(SPACE); // space separator for Splunk
	    sbuf.append(key.toString());
	    sbuf.append(SPACE); // space separator for Splunk
	    sbuf.append(value.toString());
	    sbuf.append("\n");
	    String eventString = sbuf.toString();
	    writerOut.write(eventString);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
		InterruptedException {
	    writerOut.close();
	}
    }

    private void loginSplunk(Configuration conf, String jobName) {
	try {
	    if (service == null) {
		// build up login
		Args args = new Args();
		args.put("username", conf.get(SplunkConfiguration.USERNAME));
		args.put("password", conf.get(SplunkConfiguration.PASSWORD));
		args.put("host", conf.get(SplunkConfiguration.SPLUNKHOST));
		args.put("port",
			conf.getInt(SplunkConfiguration.SPLUNKPORT, 8089));
		service = Service.connect(args);
	    }
	    if (stream == null) {
		// create an http stream input into an index configured by
		// JobConf.
		Index index = service.getIndexes().get(
			conf.get(SplunkConfiguration.SPLUNKINDEX));
		Args attachArgs = new Args();
		attachArgs.put("source", jobName);
		attachArgs.put("sourcetype",
			conf.get(SplunkConfiguration.SPLUNKSOURCETYPE));
		stream = index.attach(attachArgs);
		OutputStream ostream = stream.getOutputStream();
		writerOut = new OutputStreamWriter(ostream, "UTF8");
	    }
	} catch (Exception e) {
	    LOG.error(e);
	    e.printStackTrace();
	    throw new RuntimeException("Failed to connect to splunk, "
		    + "or connect to streaming socket", e);
	}
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
	    throws IOException, InterruptedException {
	loginSplunk(context.getConfiguration(), context.getJobName());

	return new SplunkRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException,
	    InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
	    throws IOException, InterruptedException {
	if (committer == null) {
	    Path output = getOutputPath(context);
	    committer = new FileOutputCommitter(output, context);
	}
	return committer;
    }

    public static Path getOutputPath(JobContext job) {
	String name = job.getConfiguration().get("mapred.output.dir");
	return name == null ? null : new Path(name);
    }

}
