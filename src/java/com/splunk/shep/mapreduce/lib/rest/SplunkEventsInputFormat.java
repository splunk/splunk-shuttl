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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * @author kpakkirisamy, hyan
 * 
 */
public class SplunkEventsInputFormat extends
	FileInputFormat<LongWritable, Text> {
    private static Log LOG = LogFactory.getLog(SplunkEventsInputFormat.class);

    public class SplunkJsonRecordReader extends
	    RecordReader<LongWritable, Text> {

	LongWritable key;
	Text value;
	FSDataInputStream fis;
	long splitSize;
	DataOutputBuffer buffer = new DataOutputBuffer();

	public SplunkJsonRecordReader(FileSplit split, Configuration conf)
		throws IOException {
	    Path file = split.getPath();
	    FileSystem fs = file.getFileSystem(conf);
	    this.fis = fs.open(split.getPath());
	    LOG.trace("Path: " + file);
	}

	@Override
	public void close() throws IOException {
	    this.fis.close();
	}

	@Override
	public float getProgress() throws IOException {
	    return ((float) 0);
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
	    Path path = ((FileSplit) split).getPath();
	    this.splitSize = split.getLength();
	    FileSystem fs = path.getFileSystem(context.getConfiguration());
	    this.fis = fs.open(path);
	    LOG.debug("Path: " + path);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
	    if (key == null) {
		key = new LongWritable();
	    }
	    if (value == null) {
		value = new Text();
	    }
	    try {
		key.set(this.fis.getPos());
		String event = fis.readUTF();
		if (event == null) {
		    return false;
		}
		LOG.trace("event: " + event);
		value.set(event);
	    } catch (java.io.EOFException e) {
		return false;
	    }
	    return true;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
		InterruptedException {
	    return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
	    return value;
	}
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
	List<InputSplit> splits = new ArrayList<InputSplit>();
	for (FileStatus status : listStatus(job)) {
	    Path fileName = status.getPath();
	    if (status.isDir()) {
		throw new IOException("Not a file: " + fileName);
	    }
	    LOG.trace("Adding split: " + fileName);
	    splits.add(new FileSplit(fileName, 0, status.getLen(),
		    new String[] {}));
	}
	return splits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
	    InputSplit split, TaskAttemptContext context) throws IOException,
	    InterruptedException {
	return new SplunkJsonRecordReader((FileSplit) split,
		context.getConfiguration());
    }
}
