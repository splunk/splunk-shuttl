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
package com.splunk.shuttl.server.model;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Persistence class for Archiver configuration xml
 * 
 * @author kpakkirisamy
 * 
 */
@XmlRootElement(namespace = "com.splunk.shuttl.server.model")
@XmlType(propOrder = { "localArchiverDir", "archiveFormats", "clusterName",
		"serverName", "bucketFormatPriority", "backendName", "archivePath",
		"archiverRootURI" })
public class ArchiverConf {
	private String localArchiverDir;
	private List<ArchiveFormat> archiveFormats;
	private String clusterName;
	private String serverName;
	private List<String> bucketFormatPriority;
	private String backendName;
	private String archivePath;
	private String archiverRootURI;

	public String getLocalArchiverDir() {
		return localArchiverDir;
	}

	public void setLocalArchiverDir(String localArchiverDir) {
		this.localArchiverDir = localArchiverDir;
	}

	@XmlElementWrapper(name = "archiveFormats")
	@XmlElements({ @XmlElement(name = "archiveFormat", type = ArchiveFormat.class) })
	public List<ArchiveFormat> getArchiveFormats() {
		return archiveFormats;
	}

	public void setArchiveFormats(List<ArchiveFormat> formats) {
		this.archiveFormats = formats;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public List<String> getBucketFormatPriority() {
		return bucketFormatPriority;
	}

	public void setBucketFormatPriority(List<String> priorityList) {
		this.bucketFormatPriority = priorityList;
	}

	public String getBackendName() {
		return backendName;
	}

	public void setBackendName(String backendName) {
		this.backendName = backendName;
	}

	public String getArchivePath() {
		return archivePath;
	}

	public void setArchivePath(String archivePath) {
		this.archivePath = archivePath;
	}

	public String getArchiverRootURI() {
		return archiverRootURI;
	}

	public void setArchiverRootURI(String archiverRootURI) {
		this.archiverRootURI = archiverRootURI;
	}

}
