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
package com.splunk.shep.export;

import static com.splunk.shep.ShepConstants.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;

/**
 * @author hyan
 *
 */
public class TranslogService {
    // what is this time, Splunk birthday? Sun, 09 Sep 2001 01:46:40 GMT
    public static final long DEFAULT_ENDTIME = 1000000000;

    private PropertiesConfiguration config;

    public TranslogService() throws IOException {
	this(TRANSLOG_FILE_PATH);
    }

    public TranslogService(String translogFilePath) throws IOException {
	File translog = new File(translogFilePath);

	createTranslog(translog);

	try {
	    config = new PropertiesConfiguration(translog);
	    config.load();
	} catch (ConfigurationException e) {
	    throw new IOException("Failed to open ");
	}
    }

    private void createTranslog(File translog) throws IOException {
	if (!translog.exists()) {
	    FileUtils.forceMkdir(new File(translog.getParent()));
	    translog.createNewFile();
	    // FileUtils.write(translog, TRANSLOG_ENDTIME_KEY + "="
	    // + DEFAULT_ENDTIME);
	}
    }

    public synchronized long getEndTime(String indexName) {
	String key = indexName + "." + TRANSLOG_ENDTIME_KEY;
	long value = DEFAULT_ENDTIME;
	if (config.containsKey(key)) {
	    value = config.getLong(key, DEFAULT_ENDTIME);
	}
	return value;
    }
    
    public synchronized void setEndTime(String indexName, long endTime)
	    throws IOException {
	String key = indexName + "." + TRANSLOG_ENDTIME_KEY;
	if (config.containsKey(key)) {
	    config.setProperty(key, endTime);
	} else {
	    config.addProperty(key, endTime);
	}
	try {
	    config.save();
	} catch (ConfigurationException e) {
	    throw new IOException("Failed to update property " + key);
	}
    }

}
