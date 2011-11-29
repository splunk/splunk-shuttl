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

package com.splunk.shep.mapreduce.lib.rest.tests.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.jhu.nlp.wikipedia.PageCallbackHandler;
import edu.jhu.nlp.wikipedia.WikiPage;

/**
 * An even simpler callback demo. qa
 * 
 * @author Kiru Pakkirisamy (based on a sample by Jason Smith)
 * @see PageCallbackHandler
 * 
 */

public class WikiPageSAXHandler implements PageCallbackHandler {
    int count = 0;
    FileOutputStream fs = null;
    SequenceFile.Writer writer = null;
    org.apache.hadoop.io.SequenceFile.Reader reader = null;
    Text key = new Text();
    Text value = new Text();
    MapWritable value2 = new MapWritable();

    public WikiPageSAXHandler(String filename) {
	try {
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(filename), conf);
	    Path path = new Path(filename);
	    if (fs.exists(path)) {
		// remove the file first
		fs.delete(path);
	    }
	    this.writer = SequenceFile.createWriter(fs, conf, path,
		    org.apache.hadoop.io.Text.class,
		    org.apache.hadoop.io.MapWritable.class);
	} catch (Exception e) {
	    e.printStackTrace();
	}

    }

    public WikiPageSAXHandler(String filename, boolean read) {
	try {
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(filename), conf);
	    Path path = new Path(filename);
	    this.reader = new SequenceFile.Reader(fs, path, conf);
	} catch (Exception e) {
	    e.printStackTrace();
	}

    }

    public void close() {
	try {
	    IOUtils.closeStream(this.writer);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public boolean next(Writable key, Writable value) throws IOException {
	return this.reader.next(key, value);
    }

    public void process(WikiPage page) {
	try {
	    key.set(page.getID());
	    addStringKeyValue("title", page.getTitle(), value2);
	    addStringKeyValue("text", page.getWikiText(), value2);
	    addStringKeyValue("author", page.getAuthor(), value2);
	    addStringKeyValue("modifiedtime", page.getModifiedTime(), value2);
	    writer.append(key, value2);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    void addStringKeyValue(String key, String value, MapWritable map) {
	Text mapkey = new Text();
	Text mapvalue = new Text();
	mapkey.set(key);
	mapvalue.set(value);
	map.put(mapkey, mapvalue);
    }
}