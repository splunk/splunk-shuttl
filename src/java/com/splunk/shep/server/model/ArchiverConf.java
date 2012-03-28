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
@XmlRootElement(namespace = "com.splunk.shep.server.model")
@XmlType(propOrder = { "archiverRoot", "clusterName", "indexNames" })
public class ArchiverConf implements ArchiverConfiguration {
    private String archiverRoot;
    private String clusterName;
    private List<String> indexNames;

    /* (non-Javadoc)
     * @see com.splunk.shep.server.model.ArchiverConfInterface#getArchiverRoot()
     */
    @Override
    public String getArchiverRoot() {
	return archiverRoot;
    }

    /* (non-Javadoc)
     * @see com.splunk.shep.server.model.ArchiverConfInterface#setArchiverRoot(java.lang.String)
     */
    @Override
    public void setArchiverRoot(String archiverRoot) {
	this.archiverRoot = archiverRoot;
    }

    /* (non-Javadoc)
     * @see com.splunk.shep.server.model.ArchiverConfInterface#getClusterName()
     */
    @Override
    public String getClusterName() {
	return clusterName;
    }

    /* (non-Javadoc)
     * @see com.splunk.shep.server.model.ArchiverConfInterface#setClusterName(java.lang.String)
     */
    @Override
    public void setClusterName(String clusterName) {
	this.clusterName = clusterName;
    }

    @XmlElementWrapper(name = "indexNames")
    @XmlElement(name = "indexName")
    public List<String> getIndexNames() {
	return indexNames;
    }

    public void setIndexNames(List<String> indexNames) {
	this.indexNames = indexNames;
    }
}
