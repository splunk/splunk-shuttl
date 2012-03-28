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
package com.splunk.shep.export.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;

import com.splunk.shep.ShepConstants.SystemType;
import com.splunk.shep.export.TranslogService;

/**
 * @author hyan
 *
 */
public abstract class EventWriter {
    protected TranslogService translogService;
    protected boolean flushed;

    /**
     * @param event
     * @throws IOException
     */
    public abstract void write(String event) throws IOException;

    /**
     * @param is
     * @throws IOException
     */
    public abstract void write(InputStream is) throws IOException;

    /**
     * @throws IOException
     */
    public abstract void close() throws IOException;

    /**
     * @throws IOException
     */
    public abstract void flush() throws IOException;

    public static EventWriter getInstance(SystemType type, String fileName,
	    boolean append, Configuration conf) throws IOException,
	    IllegalArgumentException {
	switch (type) {
	case local:
	    return new LocalEventWriter(fileName, append);
	case hdfs:
	    return new HdfsEventWriter(fileName, append, conf);
	}
	throw new IllegalArgumentException("Unsupported SystemType: "
		+ type);
    }


}
