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
package com.splunk.shuttl.server.mbeans;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.splunk.shuttl.server.mbeans.util.JAXBUtils;
import com.splunk.shuttl.server.model.ServerConf;

/**
 * 
 * @author kpakkirisamy
 * 
 */
public class ShuttlServer implements ShuttlServerMBean {
	// error messages
	private static final String M_BEAN_NOT_INITIALIZED_WITH_XML_BEAN = "MBean not initialized with xml bean";
	private static final String CLUSTER_NOT_FOUND = "Cluster not found ";
	private static final String SHUTTL_SERVER_INIT_FAILURE = "ShuttlServer init failure";
	// end error messages
	private static String SERVERCONF_XML = "etc/apps/shuttl/conf/server.xml";
	private Logger logger = Logger.getLogger(getClass());
	private String defHadoopClusterHost = null;
	private int defHadoopClusterPort = -1;
	private String xmlFilePath;
	private ServerConf conf;

	public ShuttlServer() throws ShuttlMBeanException {
		try {
			this.xmlFilePath = System.getProperty("splunk.home") + SERVERCONF_XML;
			refresh();
		} catch (Exception e) {
			logger.error(SHUTTL_SERVER_INIT_FAILURE, e);
			throw new ShuttlMBeanException(e);
		}
	}

	// this signature used by tests
	public ShuttlServer(String confFilePath) throws ShuttlMBeanException {
		try {
			this.xmlFilePath = confFilePath;
			refresh();
		} catch (Exception e) {
			logger.error(SHUTTL_SERVER_INIT_FAILURE, e);
			throw new ShuttlMBeanException(e);
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
	public String getShuttlHostName() throws ShuttlMBeanException {
		try {
			InetAddress addr = java.net.InetAddress.getLocalHost();
			return addr.getHostName();
		} catch (Exception e) {
			throw new ShuttlMBeanException(e);
		}
	}

	@Override
	public String getHadoopClusterHost(String name) throws ShuttlMBeanException {
		ArrayList<ServerConf.HadoopCluster> clusterlist = this.conf
				.getClusterlist();
		if (clusterlist == null)
			throw new ShuttlMBeanException(CLUSTER_NOT_FOUND + name);
		for (ServerConf.HadoopCluster cluster : clusterlist)
			if (cluster.getName().equals(name))
				return cluster.getHost();
		throw new ShuttlMBeanException(CLUSTER_NOT_FOUND + name);
	}

	@Override
	public void setHadoopClusterHost(String name, String host)
			throws ShuttlMBeanException {
		ArrayList<ServerConf.HadoopCluster> clusterlist = this.conf
				.getClusterlist();
		if (clusterlist == null)
			throw new ShuttlMBeanException(CLUSTER_NOT_FOUND + name);
		for (ServerConf.HadoopCluster cluster : clusterlist)
			if (cluster.getName().equals(name))
				cluster.setHost(host);
	}

	@Override
	public int getHadoopClusterPort(String name) throws ShuttlMBeanException {
		ArrayList<ServerConf.HadoopCluster> clusterlist = this.conf
				.getClusterlist();
		if (clusterlist == null)
			throw new ShuttlMBeanException(CLUSTER_NOT_FOUND + name);
		for (ServerConf.HadoopCluster cluster : clusterlist)
			if (cluster.getName().equals(name))
				return cluster.getPort();
		throw new ShuttlMBeanException(CLUSTER_NOT_FOUND + name);
	}

	@Override
	public void setHadoopClusterPort(String name, int port)
			throws ShuttlMBeanException {
		ArrayList<ServerConf.HadoopCluster> clusterlist = this.conf
				.getClusterlist();
		if (clusterlist == null)
			throw new ShuttlMBeanException(CLUSTER_NOT_FOUND + name);
		for (ServerConf.HadoopCluster cluster : clusterlist)
			if (cluster.getName().equals(name))
				cluster.setPort(port);

	}

	@Override
	public String getDefHadoopClusterHost() throws ShuttlMBeanException {
		if (this.defHadoopClusterHost == null)
			throw new ShuttlMBeanException(M_BEAN_NOT_INITIALIZED_WITH_XML_BEAN);
		return this.defHadoopClusterHost;
	}

	public void setDefHadoopClusterHost(String defHadoopClusterHost) {
		this.defHadoopClusterHost = defHadoopClusterHost;
	}

	@Override
	public int getDefHadoopClusterPort() throws ShuttlMBeanException {
		if (this.defHadoopClusterPort == -1)
			throw new ShuttlMBeanException(M_BEAN_NOT_INITIALIZED_WITH_XML_BEAN);
		return this.defHadoopClusterPort;
	}

	public void setDefHadoopClusterPort(int defHadoopClusterPort) {
		this.defHadoopClusterPort = defHadoopClusterPort;
	}

	@Override
	public String[] getHadoopClusterNames() {
		ArrayList<ServerConf.HadoopCluster> clusterlist = this.conf
				.getClusterlist();
		if (clusterlist != null) {
			String names[] = new String[clusterlist.size()];
			int count = 0;
			for (ServerConf.HadoopCluster cluster : clusterlist)
				names[count++] = cluster.getName();
			return names;
		}
		return null;
	}

	@Override
	public void addHadoopCluster(String name) {
		ArrayList<ServerConf.HadoopCluster> clusterlist = this.conf
				.getClusterlist();
		if (clusterlist == null) {
			this.conf.setClusterlist(new ArrayList<ServerConf.HadoopCluster>());
			clusterlist = this.conf.getClusterlist();
		}
		for (ServerConf.HadoopCluster cluster : clusterlist)
			if (cluster.getName().equals(name))
				// already exists - ignore
				return;
		ServerConf.HadoopCluster cluster = new ServerConf.HadoopCluster();
		cluster.setName(name);
		clusterlist.add(cluster);
	}

	@Override
	public void deleteHadoopCluster(String name) {
		ArrayList<ServerConf.HadoopCluster> clusterlist = this.conf
				.getClusterlist();
		ServerConf.HadoopCluster delcluster = null;
		for (ServerConf.HadoopCluster cluster : clusterlist)
			if (cluster.getName().equals(name))
				delcluster = cluster;
		if (delcluster != null)
			clusterlist.remove(delcluster);
	}

	@Override
	public boolean isDefault(String clustername) throws ShuttlMBeanException {
		ArrayList<ServerConf.HadoopCluster> clusterlist = this.conf
				.getClusterlist();
		if (clusterlist == null)
			throw new ShuttlMBeanException(CLUSTER_NOT_FOUND + clustername);
		for (ServerConf.HadoopCluster cluster : clusterlist)
			if (cluster.getName().equals(clustername))
				return cluster.isDefcluster();
		return false;
	}

	@Override
	public void setDefault(String clustername) throws ShuttlMBeanException {
		ArrayList<ServerConf.HadoopCluster> clusterlist = this.conf
				.getClusterlist();
		if (clusterlist == null)
			throw new ShuttlMBeanException(CLUSTER_NOT_FOUND + clustername);
		for (ServerConf.HadoopCluster cluster : clusterlist)
			if (cluster.getName().equals(clustername))
				cluster.setDefcluster(true);
	}

	@Override
	public void save() throws ShuttlMBeanException {
		try {
			JAXBUtils.save(ServerConf.class, this.conf, this.xmlFilePath);
		} catch (Exception e) {
			logger.error(e);
			throw new ShuttlMBeanException(e);
		}
	}

	@Override
	public void refresh() throws ShuttlMBeanException {
		try {
			this.conf = (ServerConf) JAXBUtils.refresh(ServerConf.class,
					this.xmlFilePath);
			ArrayList<ServerConf.HadoopCluster> clusterlist = conf.getClusterlist();
			if (clusterlist != null)
				for (ServerConf.HadoopCluster cluster : clusterlist)
					if (cluster.isDefcluster()) {
						this.defHadoopClusterHost = cluster.getHost();
						this.defHadoopClusterPort = cluster.getPort();
					}
		} catch (FileNotFoundException fnfe) {
			this.conf = new ServerConf();
		} catch (Exception e) {
			logger.error(e);
			throw new ShuttlMBeanException(e);
		}
	}

	@Override
	public ServerConf getServerConf() {
		return this.conf;
	}

	@Override
	public void setServerConf(ServerConf conf) {
		this.conf = conf;
	}
}
