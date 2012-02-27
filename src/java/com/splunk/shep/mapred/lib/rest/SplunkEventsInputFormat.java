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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class SplunkEventsInputFormat extends
	FileInputFormat<LongWritable, Text> implements JobConfigurable {
    private static Logger logger = Logger
	    .getLogger(SplunkEventsInputFormat.class);

    public RecordReader<LongWritable, Text> getRecordReader(
	    InputSplit genericSplit, JobConf job, Reporter reporter)
	    throws IOException {

	reporter.setStatus(genericSplit.toString());
	return new SplunkJsonRecordReader((FileSplit) genericSplit, job);
    }

    public class SplunkJsonRecordReader implements
	    RecordReader<LongWritable, Text> {

	FSDataInputStream fis;
	long splitsize;
	DataOutputBuffer buffer = new DataOutputBuffer();

	public SplunkJsonRecordReader(FileSplit split, JobConf jobConf)
		throws IOException {
	    Path file = split.getPath();
	    this.splitsize = split.getLength();
	    FileSystem fs = file.getFileSystem(jobConf);
	    this.fis = fs.open(split.getPath());
	    logger.trace("Path: " + file);
	}

	public LongWritable createKey() {
	    return new LongWritable();
	}

	public Text createValue() {
	    return new Text();
	}

	public long getPos() throws IOException {
	    return this.fis.getPos();
	}

	public void close() throws IOException {
	    this.fis.close();
	}

	public float getProgress() throws IOException {
	    return ((float) 0);
	}

	public boolean next(LongWritable key, Text value) throws IOException {
	    try {
		key.set(this.fis.getPos());
		String event = fis.readUTF();
		if (event == null) {
		    return false;
		}
		logger.trace("event: " + event);
		value.set(event);
	    } catch (java.io.EOFException e) {
		return false;
	    }
	    return true;
	}

	public InputSplit[] getSplits(JobConf job, int numSplits)
		throws IOException {
	    ArrayList<FileSplit> splits = new ArrayList<FileSplit>();
	    for (FileStatus status : listStatus(job)) {
		Path fileName = status.getPath();
		if (status.isDir()) {
		    throw new IOException("Not a file: " + fileName);
		}
		logger.trace("Adding split: " + fileName);
		splits.add(new FileSplit(fileName, 0, status.getLen(),
			new String[] {}));
	    }
	    return splits.toArray(new FileSplit[splits.size()]);
	}
    }

    public void configure(JobConf conf) {
    }
}
