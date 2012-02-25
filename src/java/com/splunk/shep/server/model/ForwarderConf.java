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

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Persistence class for the forwarder.xml
 * 
 * @author kpakkirisamy
 * 
 */
@XmlRootElement(namespace = "com.splunk.shep.server.model")
@XmlType(propOrder = { "hdfssinklist", "flumesinklist" })
public class ForwarderConf {

    private ArrayList<HDFSSink> hdfssinklist;

    private ArrayList<FlumeSink> flumesinklist;

    @XmlElementWrapper(name = "hdfssinklist")
    @XmlElement(name = "hdfssink")
    public ArrayList<HDFSSink> getHdfssinklist() {
	return hdfssinklist;
    }

    public void setHdfssinklist(ArrayList<HDFSSink> hdfssinklist) {
	this.hdfssinklist = hdfssinklist;
    }

    @XmlElementWrapper(name = "flumesinklist")
    @XmlElement(name = "flumesink")
    public ArrayList<FlumeSink> getFlumesinklist() {
	return flumesinklist;
    }

    public void setFlumesinklist(ArrayList<FlumeSink> flumesinklist) {
	this.flumesinklist = flumesinklist;
    }

    @XmlRootElement(name = "HDFSSink")
    @XmlType(propOrder = { "name", "cluster", "filePrefix", "fileRollingSize",
	    "useAppending" })
    public static class HDFSSink {
	private String cluster;
	private String name;
	private String filePrefix;
	private long fileRollingSize;
	private boolean useAppending;

	public long getFileRollingSize() {
	    return fileRollingSize;
	}

	public void setFileRollingSize(long fileRollingSize) {
	    this.fileRollingSize = fileRollingSize;
	}

	public boolean isUseAppending() {
	    return useAppending;
	}

	public void setUseAppending(boolean useAppending) {
	    this.useAppending = useAppending;
	}

	public String getName() {
	    return name;
	}

	public void setName(String name) {
	    this.name = name;
	}

	public String getCluster() {
	    return cluster;
	}

	public void setCluster(String cluster) {
	    this.cluster = cluster;
	}

	public String getFilePrefix() {
	    return filePrefix;
	}

	public void setFilePrefix(String fileprefix) {
	    this.filePrefix = fileprefix;
	}
    }

    @XmlRootElement(name = "FlumeSink")
    @XmlType(propOrder = { "name", "cluster", "maxEventSizeKB" })
    public static class FlumeSink {
	private String name;
	private String cluster;
	private int maxEventSizeKB = 64000;

	public String getName() {
	    return name;
	}

	public void setName(String name) {
	    this.name = name;
	}

	public int getMaxEventSizeKB() {
	    return maxEventSizeKB;
	}

	public void setMaxEventSizeKB(int maxEventSizeKB) {
	    this.maxEventSizeKB = maxEventSizeKB;
	}

	public String getCluster() {
	    return cluster;
	}

	public void setCluster(String cluster) {
	    this.cluster = cluster;
	}
    }

}
