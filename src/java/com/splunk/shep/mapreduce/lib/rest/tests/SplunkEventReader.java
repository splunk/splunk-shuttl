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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

public class SplunkEventReader {
    public static class Map extends MapReduceBase implements
	    Mapper<LongWritable, Text, Text, NullWritable> {

	public void map(LongWritable key, Text value,
		OutputCollector<Text, NullWritable> output, Reporter reporter)
		throws IOException {
	    NullWritable nullwr = NullWritable.get();
	    output.collect(value, nullwr);
	}
    }


    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(WikiLinkCount.class);
	conf.setJobName("SplunkEventReader");
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(NullWritable.class);

	conf.setMapperClass(Map.class);

	conf.setInputFormat(com.splunk.shep.mapreduce.lib.rest.SplunkEventsInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);


	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
    }

}