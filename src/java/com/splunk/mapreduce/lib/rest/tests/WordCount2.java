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

package com.splunk.mapreduce.lib.rest.tests;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.splunk.mapreduce.lib.rest.SplunkConfiguration;
import com.splunk.mapreduce.lib.rest.SplunkOutputFormat;


public class WordCount2 {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, SplunkRecord, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, SplunkRecord value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			System.out.println("got a map");
			String line = value.getMap().get("_raw");
			if (line == null) {
				System.out.println("_raw is null");
				return;
			}
			System.out.println("line " + line);
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
			
		}
		
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Starting job");
		JobConf conf = new JobConf(WordCount2.class);
		conf.setJobName("wordcount");
		SplunkConfiguration.setConnInfo(conf, "10.196.45.203", 8089, "admin", "changeme");
		//SplunkConfiguration.setSplunkQuery(conf, "source=wordfile-timestamp", "%Y-%m-%d %H:%M:%S", new String[][]{{"2011-09-19 17:04:11", "2011-09-19 17:06:40"}, {"2011-09-19 17:06:41", "2011-09-19 17:09:12"}});
		//SplunkConfiguration.setSplunkQueryByIndexers(conf, "source=wordfile-timestamp*",  new String[]{"ip-10-196-45-203.ec2.internal", "domU-12-31-39-16-C6-C0.compute-1.internal"});
		String query = "source::*wordfile-timestamp";
		String indexer1 = "localhost"; // The indexer should be configured and
										// passed in as an argument, instead of
										// being hard coded.
		SplunkConfiguration.setSplunkQueryByIndexers(conf, query,
				new String[] { indexer1 });
		conf.set(SplunkConfiguration.SPLUNKEVENTREADER, "com.splunk.mapreduce.lib.rest.tests.SplunkRecord");
		conf.setInputFormat(com.splunk.mapreduce.lib.rest.SplunkInputFormat.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(com.splunk.mapreduce.lib.rest.SplunkInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//conf.setOutputFormat(com.splunk.mapred.lib.rest.SplunkOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		System.out.println("indexbyhost " + conf.getInt(SplunkConfiguration.INDEXBYHOST,0));
		JobClient.runJob(conf);
	}
}