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
package com.splunk.shep.exporter.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author hyan
 *
 */
public class HdfsEventWriter extends EventWriter {
    private FileSystem out;
    private boolean append;
    private Path outputFile;

    public HdfsEventWriter(String fileName, boolean append, Configuration conf)
	    throws IOException {
	out = FileSystem.get(conf);
	String pathString = out.getUri().toString() + fileName;
	outputFile = new Path(pathString);
	this.append = append;
    }

    @Override
    public void write(String event) throws IOException {
	if (append) {
	    out.append(outputFile);
	} else {
	    out.create(outputFile);
	}
    }

    @Override
    public void write(InputStream is) throws IOException {

    }

    @Override
    public void close() throws IOException {
	flush();
	out.close();
    }

    @Override
    public void flush() throws IOException {

    }

}
