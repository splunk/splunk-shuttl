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
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.splunk.shep.archiver.archive.BucketFormat;

/**
 * Persistence class for Archiver configuration xml
 * 
 * @author kpakkirisamy
 * 
 */
@XmlRootElement(namespace = "com.splunk.shep.server.model")
@XmlType(propOrder = { "archiveFormat", "clusterName", "serverName",
	"indexNames", "archiverRootURI", "bucketFormatPriority", "tmpDirectory" })
public class ArchiverConf implements ArchiverConfiguration {
    private BucketFormat archiveFormat = BucketFormat.SPLUNK_BUCKET;
    private String tmpDirectory;
    private String clusterName;
    private String serverName;
    private String archiverRootURI;
    private List<String> indexNames;
    private List<BucketFormat> bucketFormatPriority = new ArrayList<BucketFormat>();

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.server.model.ArchiverConfInterface#getArchiveFormat(com
     * .splunk.shep.archiver.archive.BucketFormat)
     */
    @Override
    public String getArchiveFormat() {
	return archiveFormat.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.server.model.ArchiverConfInterface#setArchiveFormat()
     */
    @Override
    public void setArchiveFormat(String format) {
	archiveFormat = BucketFormat.valueOf(format);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.splunk.shep.server.model.ArchiverConfInterface#getClusterName()
     */
    @Override
    public String getClusterName() {
	return clusterName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.server.model.ArchiverConfInterface#setClusterName(java
     * .lang.String)
     */
    @Override
    public void setClusterName(String clusterName) {
	this.clusterName = clusterName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.splunk.shep.server.model.ArchiverConfInterface#getServerName()
     */
    @Override
    public String getServerName() {
	return serverName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.server.model.ArchiverConfInterface#setServerName(java
     * .lang.String)
     */
    @Override
    public void setServerName(String serverName) {
	this.serverName = serverName;
    }

    @XmlElementWrapper(name = "indexNames")
    @XmlElement(name = "indexName")
    public List<String> getIndexNames() {
	return indexNames;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.server.model.ArchiverConfiguration#getBucketFormatPriority
     * ()
     */
    @Override
    public List<String> getBucketFormatPriority() {
	List<String> tempList = new ArrayList<String>();
	for (BucketFormat format : bucketFormatPriority) {
	    tempList.add(format.toString());
	}
	return tempList;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.server.model.ArchiverConfiguration#setBucketFormatPriority
     * (java.util.List)
     */
    @Override
    public void setBucketFormatPriority(List<String> priorityList) {
	bucketFormatPriority = new ArrayList<BucketFormat>();
	for (String format : priorityList) {
	    bucketFormatPriority.add(BucketFormat.valueOf(format));
	}
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.splunk.shep.server.model.ArchiverConfiguration#getTmpDirectory()
     */
    @Override
    public String getTmpDirectory() {
	return tmpDirectory;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.splunk.shep.server.model.ArchiverConfiguration#setTmpDirectory()
     */
    @Override
    public void setTmpDirectory(String path) {
	tmpDirectory = path;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.server.model.ArchiverConfiguration#setArchiverHadoopURI
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
     * com.splunk.shep.server.model.ArchiverConfiguration#getArchiverHadoopURI()
     */
    @Override
    public String getArchiverRootURI() {
	return archiverRootURI;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.server.model.ArchiverConfiguration#setIndexNames(java
     * .util.List)
     */
    @Override
    public void setIndexNames(List<String> indexNames) {
	this.indexNames = indexNames;
    }
}
