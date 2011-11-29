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
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import com.splunk.shep.mapreduce.lib.rest.SplunkOutputFormat;

import edu.jhu.nlp.wikipedia.WikiTextParser;

public class Wiki2SplunkMapper {
    static final String SPACE = " ";

    public static class Map extends MapReduceBase implements
	    Mapper<Text, MapWritable, Text, Text> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Text updatedtime = new Text();
	private Text info = new Text();

	public void map(Text key, MapWritable value,
		OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {
	    StringBuffer outputstr = new StringBuffer();

	    String mtime = getValue("modifiedtime", value);
	    mtime = mtime.replaceAll("\\n", "").trim();

	    String title = getValue("title", value);
	    title = title.replaceAll("\\n", "").trim();
	    outputstr.append("[" + title + "]");
	    outputstr.append(SPACE);

	    String author = getValue("author", value);
	    author = author.replaceAll("\\n", "").trim();
	    outputstr.append("[" + author + "]");
	    outputstr.append(SPACE);

	    WikiTextParser parser = new WikiTextParser(getValue("text", value));
	    Vector links = parser.getLinks();
	    outputstr.append("[");
	    for (int i = 0; i < links.size(); i++) {
		String link = (String) links.elementAt(i);
		if (i != 0) {
		    outputstr.append(",");
		}
		outputstr.append(link);
	    }
	    outputstr.append("]");
	    outputstr.append("\n");
	    updatedtime.set(mtime);
	    info.set(outputstr.toString());
	    output.collect(updatedtime, info);
	}

	private String getValue(String keystr, MapWritable value) {
	    String text = null;
	    Text key = new Text(keystr);
	    text = value.get(key).toString();
	    return text;
	}
    }

    public static void main(String[] args) throws Exception {
	if (args.length != 6) {
	    System.out
		    .println("Usage: Wiki2SplunkMapper <inputdir> <outputdir> <splunk-host> <splunk-port> <username> <password>");
	    System.exit(1);
	}
	JobConf conf = new JobConf(WordCount.class);
	conf.setJobName("wikiinput2");
	conf.set(SplunkOutputFormat.SPLUNKHOST, args[2]);
	conf.setInt(SplunkOutputFormat.SPLUNKPORT, Integer.parseInt(args[3]));
	conf.set(SplunkOutputFormat.USERNAME, args[4]);
	conf.set(SplunkOutputFormat.PASSWORD, args[5]);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);

	conf.setMapperClass(Map.class);

	conf.setInputFormat(SequenceFileInputFormat.class);
	conf.setOutputFormat(com.splunk.shep.mapreduce.lib.rest.SplunkOutputFormat.class);
	conf.setMapRunnerClass(org.apache.hadoop.mapred.lib.MultithreadedMapRunner.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
    }
}
