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

import static com.splunk.shep.mapred.lib.rest.mock.SplunkInputFormatMock.QUERY1;
import static com.splunk.shep.mapred.lib.rest.mock.SplunkInputFormatMock.QUERY2;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.splunk.shep.mapreduce.lib.rest.mock.SplunkInputFormatMock;
import com.splunk.shep.mapreduce.lib.rest.tests.SplunkRecord;
import com.splunk.shep.testutil.SplunkServiceParameters;

/**
 * @author hyan
 * 
 */
@Test(groups = { "embedded" })
public class TestSplunkInputFormat {
    private static SplunkServiceParameters testParameters;
    private Path output = null;
    private static Log LOG = LogFactory.getLog(TestSplunkInputFormat.class);

    @BeforeClass
    public static void setUp() throws IOException, ParseException {
	String splunkHost = "localhost";
	String splunkMGMTPort = "8089";
	String splunkUsername = "admin";
	String splunkPassword = "changeme";
	testParameters = new SplunkServiceParameters(splunkUsername,
		splunkPassword, splunkHost, splunkMGMTPort);
    }

    @AfterClass
    public static void tearDown() {

    }

    @Test
    public void testSearchAllEvents() throws IOException, InterruptedException,
	    ClassNotFoundException {
	output = new Path("build/" + this.getClass().getCanonicalName()
		+ ".testSearchAllEvents");
	testQuery(QUERY1);
    }

    @Test
    public void testSearchOneEvent() throws IOException, InterruptedException,
	    ClassNotFoundException {
	output = new Path("build/" + this.getClass().getCanonicalName()
		+ ".testSearchOneEvent");
	testQuery(QUERY2);
    }

    private void testQuery(String query) throws IOException,
	    InterruptedException, ClassNotFoundException {
	FileSystem fs = runMapReduceJob(query);
	verifyOutput(query, fs);
    }

    private FileSystem runMapReduceJob(String query) throws IOException,
	    InterruptedException, ClassNotFoundException {
	Job job = new Job();
	FileSystem fs = configureJobConf(query, job);
	job.waitForCompletion(true);
	return fs;
    }

    private FileSystem configureJobConf(String query, Job job)
	    throws IOException {
	Configuration conf = job.getConfiguration();
	SplunkConfiguration.setConnInfo(conf, testParameters.host,
		    testParameters.mgmtPort, testParameters.username,
		    testParameters.password);
	String indexer1 = "localhost";
	SplunkConfiguration.setSplunkQueryByIndexers(conf, query,
		    new String[] { indexer1 });
	conf.set(SplunkConfiguration.SPLUNKEVENTREADER,
		    SplunkRecord.class.getName());

	job.setJarByClass(TestSplunkInputFormat.class);
	job.setJobName(this.getClass().getName());
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setMapperClass(TestMapper.class);
	job.setReducerClass(TestReducer.class);

	job.setInputFormatClass(SplunkInputFormatMock.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileSystem fs = FileSystem.getLocal(job.getConfiguration());
	if (fs.exists(output)) {
	    fs.delete(output, true);
	}

	FileOutputFormat.setOutputPath(job, output);
	return fs;
    }

    private void verifyOutput(String query, FileSystem fs) throws IOException {
	FSDataInputStream open = fs.open(new Path(output, "part-r-00000"));

	HashMap<String, Integer> expected = new HashMap<String, Integer>();
	if (QUERY1.equals(query)) {
	    expected.put("17:04:15", 1);
	    expected.put("17:04:14", 1);
	    expected.put("17:04:13", 1);
	    expected.put("2011-09-19", 3);
	    expected.put("a", 3);
	    expected.put("is", 3);
	    expected.put("test", 3);
	    expected.put("this", 3);
	} else if (QUERY2.equals(query)) {
	    expected.put("17:04:15", 1);
	    expected.put("2011-09-19", 1);
	    expected.put("a", 1);
	    expected.put("is", 1);
	    expected.put("test", 1);
	    expected.put("this", 1);
	}

	List<String> readLines = IOUtils.readLines(open);
	assertEquals(expected.size(), readLines.size());
	String[] kv = new String[2];
	for (String line : readLines) {
	    kv = line.split("[ \t]");
	    LOG.debug("key:" + kv[0] + ", value:" + kv[1]);
	    assertEquals(expected.get(kv[0]), new Integer(kv[1]));
	}
    }

    private static class TestMapper extends
	    Mapper<LongWritable, SplunkRecord, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, SplunkRecord value, Context context)
		throws IOException, InterruptedException {
	    String line = value.getMap().get("_raw");
	    if (line == null) {
		LOG.debug("_raw is null");
		return;
	    }
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    while (tokenizer.hasMoreTokens()) {
		word.set(tokenizer.nextToken());
		context.write(word, one);
	    }
	}
    }

    private static class TestReducer extends
	    Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
		Context context) throws IOException, InterruptedException {
	    int sum = 0;
	    for (IntWritable v : values) {
		sum += v.get();
	    }
	    context.write(key, new IntWritable(sum));
	}
    }
}
