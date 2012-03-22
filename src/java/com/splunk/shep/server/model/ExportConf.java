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
package com.splunk.shep.server.model;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author hyan
 *
 */
@XmlRootElement(namespace = "com.splunk.shep.server.model")
public class ExportConf {
    private String outputPath;
    private String tempPath;
    private List<Channel> channels;

    public String getOutputPath() {
	return outputPath;
    }

    public void setOutputPath(String outputPath) {
	this.outputPath = outputPath;
    }

    public String getTempPath() {
	return tempPath;
    }

    public void setTempPath(String tempPath) {
	this.tempPath = tempPath;
    }

    public List<Channel> getChannels() {
	return channels;
    }

    public void setChannels(List<Channel> channels) {
	this.channels = channels;
    }

    @XmlRootElement(name = "Channel")
    public static class Channel {
	private String indexName;
	private long scheduleInterval;
	private String outputMode;// splunk-java-sdk/REST currently support
				  // csv, xml, json and raw, default is xml
	private String outputFileSystem;

	public String getIndexName() {
	    return indexName;
	}

	public void setIndexName(String indexName) {
	    this.indexName = indexName;
	}

	public long getScheduleInterval() {
	    return scheduleInterval;
	}

	public void setScheduleInterval(long scheduleInterval) {
	    this.scheduleInterval = scheduleInterval;
	}

	public String getOutputMode() {
	    return outputMode;
	}

	public void setOutputMode(String outputMode) {
	    this.outputMode = outputMode;
	}

	public String getOutputFileSystem() {
	    return outputFileSystem;
	}

	public void setOutputFileSystem(String outputFileSystem) {
	    this.outputFileSystem = outputFileSystem;
	}
    }
}
