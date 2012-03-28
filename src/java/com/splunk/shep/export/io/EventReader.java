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
import java.util.Map;

import com.splunk.shep.ShepConstants.SystemType;

/**
 * @author hyan
 *
 */
public abstract class EventReader {

    protected long endTime;

    public static EventReader getInstance(SystemType type)
	    throws IllegalArgumentException {
	switch (type) {
	case splunk:
	    return new SplunkEventReader();
	default:
	    throw new IllegalArgumentException("Unsupported SystemType: "
		    + type);
	}
    }

    public long getEndTime() {
	return endTime;
    }

    /**
     * @param indexName
     * @param lastEndTime
     * @param params
     * @return
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public abstract InputStream export(String indexName, long lastEndTime,
	    Map<String, Object> params) throws IllegalArgumentException,
	    IOException;

}
