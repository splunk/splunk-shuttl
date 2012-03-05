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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import com.splunk.shep.mapred.lib.rest.tests.WikiLinkCount;

/**
 * 
 * @author hyan
 * 
 */
public class SplunkEventReader {
    public static class SplunkEventReaderMap extends
	    Mapper<LongWritable, Text, Text, NullWritable> {

	public void map(LongWritable key, Text value, Context context)
		throws IOException {
	    NullWritable nullwr = NullWritable.get();
	    try {
		FEvent event = getEventObject(value.toString());
		value.set(event.getBody());
		context.write(value, nullwr);
	    } catch (Exception e) {
		throw new IOException(e);
	    }
	}

	private FEvent getEventObject(String item) throws Exception {
	    JsonFactory f = new JsonFactory();
	    JsonParser jp = f.createJsonParser(item);
	    FEvent event = new FEvent();
	    jp.nextToken(); // will return JsonToken.START_OBJECT (verify?)
	    while (jp.nextToken() != JsonToken.END_OBJECT) {
		String fieldname = jp.getCurrentName();
		jp.nextToken(); // move to value, or START_OBJECT/START_ARRAY
		if ("fields".equals(fieldname)) {
		    FEvent.Fields field = new FEvent.Fields();
		    while (jp.nextToken() != JsonToken.END_OBJECT) {
			String namefield = jp.getCurrentName();
			jp.nextToken(); // move to value
			if ("source".equals(namefield)) {
			    field.setSource(jp.getText());
			} else if ("sourceType".equals(namefield)) {
			    field.setSourceType(jp.getText());
			} else {
			    throw new IllegalStateException(
				    "Unrecognized field '" + fieldname + "'!");
			}
		    }
		    event.setFields(field);
		} else if ("body".equals(fieldname)) {
		    event.setBody(jp.getText());
		} else if ("host".equals(fieldname)) {
		    event.setHost(jp.getText());
		} else if ("timestamp".equals(fieldname)) {
		    event.setTimestamp(jp.getText());
		}
	    }
	    return event;
	}
    }

    public static void main(String[] args) throws Exception {
	Job job = new Job();
	// FIXME job is WikiLinkCount or SplunkEventReader?
	job.setJarByClass(WikiLinkCount.class);
	job.setJobName("SplunkEventReader");
	// JobConf conf = new JobConf(WikiLinkCount.class);
	// conf.setJobName("SplunkEventReader");
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);

	job.setMapperClass(SplunkEventReaderMap.class);

	job.setInputFormatClass(SplunkEventsInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class FEvent {
    private String body;
    private String timestamp;
    private String host;
    private Fields fields;

    public Fields getFields() {
	return this.fields;
    }

    public void setFields(Fields fields) {
	this.fields = fields;
    }

    public String getBody() {
	return body;
    }

    public void setBody(String body) {
	this.body = body;
    }

    public String getTimestamp() {
	return timestamp;
    }

    public void setTimestamp(String timestamp) {
	this.timestamp = timestamp;
    }

    public String getHost() {
	return host;
    }

    public void setHost(String host) {
	this.host = host;
    }

    public static class Fields {
	private String sourceType;
	private String source;

	public String getSourceType() {
	    return sourceType;
	}

	public void setSourceType(String sourceType) {
	    this.sourceType = sourceType;
	}

	public String getSource() {
	    return source;
	}

	public void setSource(String source) {
	    this.source = source;
	}

    }

}
