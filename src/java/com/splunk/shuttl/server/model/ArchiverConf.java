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
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Persistence class for Archiver configuration xml
 * 
 * @author kpakkirisamy
 * 
 */
@XmlRootElement(namespace = "com.splunk.shuttl.server.model")
@XmlType(propOrder = { "archiveFormats", "clusterName", "serverName",
		"archiverRootURI", "bucketFormatPriority" })
public class ArchiverConf implements ArchiverConfiguration {
	private List<String> archiveFormats;
	private String clusterName;
	private String serverName;
	private String archiverRootURI;
	private List<String> bucketFormatPriority;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.server.model.ArchiverConfInterface#getArchiveFormat(com
	 * .splunk.shuttl.archiver.archive.BucketFormat)
	 */
	@Override
	@XmlElementWrapper(name = "archiveFormats")
	@XmlElement(name = "archiveFormat")
	public List<String> getArchiveFormats() {
		return archiveFormats;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.server.model.ArchiverConfInterface#setArchiveFormat()
	 */
	@Override
	public void setArchiveFormats(List<String> formats) {
		this.archiveFormats = formats;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.splunk.shuttl.server.model.ArchiverConfInterface#getClusterName()
	 */
	@Override
	public String getClusterName() {
		return clusterName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.server.model.ArchiverConfInterface#setClusterName(java
	 * .lang.String)
	 */
	@Override
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.splunk.shuttl.server.model.ArchiverConfInterface#getServerName()
	 */
	@Override
	public String getServerName() {
		return serverName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.server.model.ArchiverConfInterface#setServerName(java
	 * .lang.String)
	 */
	@Override
	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.server.model.ArchiverConfiguration#getBucketFormatPriority
	 * ()
	 */
	@Override
	public List<String> getBucketFormatPriority() {
		return bucketFormatPriority;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.server.model.ArchiverConfiguration#setBucketFormatPriority
	 * (java.util.List)
	 */
	@Override
	public void setBucketFormatPriority(List<String> priorityList) {
		this.bucketFormatPriority = priorityList;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.server.model.ArchiverConfiguration#setArchiverHadoopURI
	 * (java.net.URI)
	 */
	@Override
	public void setArchiverRootURI(String URI) {
		archiverRootURI = URI;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.server.model.ArchiverConfiguration#getArchiverHadoopURI
	 * ()
	 */
	@Override
	public String getArchiverRootURI() {
		return archiverRootURI;
	}

}
