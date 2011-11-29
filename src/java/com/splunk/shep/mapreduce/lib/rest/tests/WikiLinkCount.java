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

package com.splunk.shep.mapreduce.lib.rest.tests;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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

import com.splunk.shep.mapreduce.lib.rest.SplunkConfiguration;

public class WikiLinkCount {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, SplunkRecord, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, SplunkRecord value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			System.out.println("got a map");
			String line = value.getMap().get("_raw");
			System.out.println("line " + line);
			StringTokenizer tokenizer = new StringTokenizer(line, "[]");
			String lasttoken = ""; // comma separated list of links

			while (tokenizer.hasMoreTokens()) {
				lasttoken = tokenizer.nextToken();
			}

			System.out.println("lasttoken " + lasttoken);
			tokenizer = new StringTokenizer(lasttoken, ",");
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
		if (args.length < 7) {
			System.out
					.println("Usage:  WikiLinkCount <inputdir> <outputdir> <splunk-host> <userid> <password> <search string>  <indexer1>  ...");
			System.exit(1);
		}
		JobConf conf = new JobConf(WikiLinkCount.class);
		conf.setJobName("WikiLinkCount");
		SplunkConfiguration.setConnInfo(conf, args[2], 8089, args[3], args[4]);
		String indexers[] = new String[args.length - 6];
		for (int i = 6; i < args.length; i++) {
			indexers[i - 6] = args[i];
		}
		SplunkConfiguration.setSplunkQueryByIndexers(conf, args[5], indexers);
		conf.set(SplunkConfiguration.SPLUNKEVENTREADER,
				SplunkRecord.class.getName());

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(com.splunk.shep.mapreduce.lib.rest.SplunkInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapRunnerClass(org.apache.hadoop.mapred.lib.MultithreadedMapRunner.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}