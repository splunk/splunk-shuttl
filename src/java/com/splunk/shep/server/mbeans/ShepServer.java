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
package com.splunk.shep.server.mbeans;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.splunk.shep.server.mbeans.util.JAXBUtils;
import com.splunk.shep.server.model.ServerConf;

/**
 * 
 * @author kpakkirisamy
 * 
 */
public class ShepServer implements ShepServerMBean {
    // error messages
    private static final String M_BEAN_NOT_INITIALIZED_WITH_XML_BEAN = "MBean not initialized with xml bean";
    private static final String CLUSTER_NOT_FOUND = "Cluster not found ";
    private static final String SHEP_SERVER_INIT_FAILURE = "ShepServer init failure";
    // end error messages
    private static String SERVERCONF_XML = "etc/apps/shep/conf/server.xml";
    private Logger logger = Logger.getLogger(getClass());
    private String defHadoopClusterHost = null;
    private int defHadoopClusterPort = -1;
    private ArrayList<ServerConf.HadoopCluster> clusterlist;
    private String xmlFilePath;
    private ServerConf conf;

    public ShepServer() throws ShepMBeanException {
	try {
	    this.xmlFilePath = System.getProperty("splunk.home")
		    + SERVERCONF_XML;
	    refresh();
	} catch (Exception e) {
	    logger.error(SHEP_SERVER_INIT_FAILURE, e);
	    throw new ShepMBeanException(e);
	}
    }
    
    // this signature used by tests
    public ShepServer(String confFilePath) throws ShepMBeanException {
	try {
	    this.xmlFilePath = confFilePath;
	    refresh();
	} catch (Exception e) {
	    logger.error(SHEP_SERVER_INIT_FAILURE, e);
	    throw new ShepMBeanException(e);
	}
    }

    @Override
    public String getHttpHost() {
	return this.conf.getHttpHost();
    }

    @Override
    public void setHttpHost(String host) {
	this.conf.setHttpHost(host);
    }

    @Override
    public int getHttpPort() {
	return this.conf.getHttpPort();
    }

    @Override
    public void setHttpPort(int port) {
	this.conf.setHttpPort(port);
    }

    @Override
    public String getSplunkClusterName() {
	return this.conf.getSplunkClusterName();
    }

    @Override
    public void setSplunkClusterName(String name) {
	this.conf.setSplunkClusterName(name);
    }

    @Override
    public String getShepHostName() throws ShepMBeanException {
	try {
	    InetAddress addr = java.net.InetAddress.getLocalHost();
	    return addr.getHostName();
	} catch (Exception e) {
	    throw new ShepMBeanException(e);
	}
    }

    @Override
    public String getHadoopClusterHost(String name) throws ShepMBeanException {
	if (this.clusterlist == null) {
	    throw new ShepMBeanException(CLUSTER_NOT_FOUND + name);
	}
	for (ServerConf.HadoopCluster cluster : this.clusterlist) {
	    if (cluster.getName().equals(name)) {
		return cluster.getHost();
	    }
	}
	throw new ShepMBeanException(CLUSTER_NOT_FOUND + name);
    }

    @Override
    public void setHadoopClusterHost(String name, String host)
	    throws ShepMBeanException {
	if (this.clusterlist == null) {
	    throw new ShepMBeanException(CLUSTER_NOT_FOUND + name);
	}
	for (ServerConf.HadoopCluster cluster : this.clusterlist) {
	    if (cluster.getName().equals(name)) {
		cluster.setHost(host);
	    }
	}
    }

    @Override
    public int getHadoopClusterPort(String name) throws ShepMBeanException {
	if (this.clusterlist == null) {
	    throw new ShepMBeanException(CLUSTER_NOT_FOUND + name);
	}
	for (ServerConf.HadoopCluster cluster : this.clusterlist) {
	    if (cluster.getName().equals(name)) {
		return cluster.getPort();
	    }
	}
	throw new ShepMBeanException(CLUSTER_NOT_FOUND + name);
    }

    @Override
    public void setHadoopClusterPort(String name, int port)
	    throws ShepMBeanException {
	if (this.clusterlist == null) {
	    throw new ShepMBeanException(CLUSTER_NOT_FOUND + name);
	}
	for (ServerConf.HadoopCluster cluster : this.clusterlist) {
	    if (cluster.getName().equals(name)) {
		cluster.setPort(port);
	    }
	}

    }

    @Override
    public String getDefHadoopClusterHost() throws ShepMBeanException {
	if (this.defHadoopClusterHost == null) {
	    throw new ShepMBeanException(M_BEAN_NOT_INITIALIZED_WITH_XML_BEAN);
	}
	return this.defHadoopClusterHost;
    }

    public void setDefHadoopClusterHost(String defHadoopClusterHost) {
	this.defHadoopClusterHost = defHadoopClusterHost;
    }

    @Override
    public int getDefHadoopClusterPort() throws ShepMBeanException {
	if (this.defHadoopClusterPort == -1) {
	    throw new ShepMBeanException(M_BEAN_NOT_INITIALIZED_WITH_XML_BEAN);
	}
	return this.defHadoopClusterPort;
    }

    public void setDefHadoopClusterPort(int defHadoopClusterPort) {
	this.defHadoopClusterPort = defHadoopClusterPort;
    }


    @Override
    public String[] getHadoopClusterNames() {
	if (this.clusterlist != null) {
	    String names[] = new String[this.clusterlist.size()];
	    int count = 0;
	    for (ServerConf.HadoopCluster cluster : this.clusterlist) {
		names[count++] = cluster.getName();
	    }
	    return names;
	}
	return null;
    }

    @Override
    public void addHadoopCluster(String name) {
	if (this.clusterlist == null) {
	    this.clusterlist = new ArrayList<ServerConf.HadoopCluster>();
	    this.conf.setClusterlist(this.clusterlist);
	}
	for (ServerConf.HadoopCluster cluster : this.clusterlist) {
	    if (cluster.getName().equals(name)) {
		// already exists - ignore
		return;
	    }
	}
	ServerConf.HadoopCluster cluster = new ServerConf.HadoopCluster();
	cluster.setName(name);
	this.clusterlist.add(cluster);
    }

    @Override
    public void deleteHadoopCluster(String name) {
	ServerConf.HadoopCluster delcluster = null;
	for (ServerConf.HadoopCluster cluster : this.clusterlist) {
	    if (cluster.getName().equals(name)) {
		delcluster = cluster;
	    }
	}
	if (delcluster != null) {
	    this.clusterlist.remove(delcluster);
	}
    }

    @Override
    public boolean isDefault(String clustername) throws ShepMBeanException {
	if (this.clusterlist == null) {
	    throw new ShepMBeanException(CLUSTER_NOT_FOUND + clustername);
	}
	for (ServerConf.HadoopCluster cluster : this.clusterlist) {
	    if (cluster.getName().equals(clustername)) {
		return cluster.isDefcluster();
	    }
	}
	return false;
    }

    @Override
    public void setDefault(String clustername) throws ShepMBeanException {
	if (this.clusterlist == null) {
	    throw new ShepMBeanException(CLUSTER_NOT_FOUND + clustername);
	}
	for (ServerConf.HadoopCluster cluster : this.clusterlist) {
	    if (cluster.getName().equals(clustername)) {
		cluster.setDefcluster(true);
	    }
	}
    }

    @Override
    public void save() throws ShepMBeanException {
	try {
	    JAXBUtils.save(ServerConf.class, this.conf, this.xmlFilePath);
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepMBeanException(e);
	}
    }

    @Override
    public void refresh() throws ShepMBeanException {
	try {
	    this.conf = (ServerConf) JAXBUtils.refresh(ServerConf.class,
		    this.xmlFilePath);
	    this.clusterlist = conf.getClusterlist();
	    if (this.clusterlist != null) {
		for (ServerConf.HadoopCluster cluster : this.clusterlist) {
		    if (cluster.isDefcluster()) {
			this.defHadoopClusterHost = cluster.getHost();
			this.defHadoopClusterPort = cluster.getPort();
		    }
		}
	    }
	} catch (FileNotFoundException fnfe) {
	    this.conf = new ServerConf();
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepMBeanException(e);
	}
    }
}
