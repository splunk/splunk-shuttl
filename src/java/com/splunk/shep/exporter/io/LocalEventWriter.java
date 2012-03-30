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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

/**
 * @author hyan
 *
 */
public class LocalEventWriter extends EventWriter {
    private BufferedWriter out;

    public LocalEventWriter(String fileName, boolean append)
	    throws IOException {
	out = new BufferedWriter(new FileWriter(fileName, append));
    }

    @Override
    public void write(String content) throws IOException {
	if (content != null) {
	    out.write(content);
	}
    }

    @Override
    public void write(InputStream is) throws IOException {
	IOUtils.copy(is, out);
    }

    @Override
    public void close() throws IOException {
	flush();
	out.close();
    }

    @Override
    public void flush() throws IOException {
	if (flushed) {
	    return;
	}
	out.flush();
    }
}
