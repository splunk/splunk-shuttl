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
 * Persistence class for the server xml
 * 
 * @author kpakkirisamy
 * 
 */
@XmlRootElement(namespace = "com.splunk.shep.server.model")
public class ServerConf {
    private ArrayList<HadoopCluster> clusterlist;

    @XmlElementWrapper(name = "clusterlist")
    @XmlElement(name = "hadoopcluster")
    public ArrayList<HadoopCluster> getClusterlist() {
	return clusterlist;
    }

    public void setClusterlist(ArrayList<HadoopCluster> clusterlist) {
	this.clusterlist = clusterlist;
    }

    @XmlRootElement(name = "HadoopCluster")
    @XmlType(propOrder = { "name", "defcluster", "host", "port" })
    public static class HadoopCluster {
	private String host;
	private String name;
	private int port;
	private boolean defcluster;

	public boolean isDefcluster() {
	    return defcluster;
	}

	public void setDefcluster(boolean defcluster) {
	    this.defcluster = defcluster;
	}

	public int getPort() {
	    return port;
	}

	public void setPort(int port) {
	    this.port = port;
	}

	public String getHost() {
	    return host;
	}

	public void setHost(String host) {
	    this.host = host;
	}

	public String getName() {
	    return name;
	}

	public void setName(String name) {
	    this.name = name;
	}
    }

}
