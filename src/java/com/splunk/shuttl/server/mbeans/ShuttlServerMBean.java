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

import com.splunk.shuttl.server.model.ServerConf;

/**
 * 
 * @author kpakkirisamy
 * 
 */
public interface ShuttlServerMBean {

	/**
	 * 
	 * @return String The default HDFS cluster host name
	 */
	public String getDefHadoopClusterHost() throws ShuttlMBeanException;

	/**
	 * 
	 * @return String The default HDFS cluster port
	 */
	public int getDefHadoopClusterPort() throws ShuttlMBeanException;

	/**
	 * Name of the host on which is running
	 * 
	 * @return
	 * @throws ShuttlMBeanException
	 */
	public String getShuttlHostName() throws ShuttlMBeanException;

	/**
	 * Returns an array of Hadoop Cluster Names
	 * 
	 * @return
	 */
	public String[] getHadoopClusterNames();

	/**
	 * adds a Hadoop Cluster configuration
	 * 
	 * @param name
	 */
	public void addHadoopCluster(String name);

	/**
	 * Deletes a Hadoop Cluster
	 * 
	 * @param name
	 */
	public void deleteHadoopCluster(String name);

	/**
	 * Checks whether a given Hadoop Cluster is the default
	 * 
	 * @param clustername
	 * @return
	 * @throws ShuttlMBeanException
	 */
	public boolean isDefault(String clustername) throws ShuttlMBeanException;

	/**
	 * Sets a cluster as the default to work with
	 * 
	 * @param clustername
	 * @throws ShuttlMBeanException
	 */
	public void setDefault(String clustername) throws ShuttlMBeanException;

	/**
	 * 
	 * @param name
	 *          The name of the cluster
	 * @return String HDFS cluster host name (namenode)
	 */
	public String getHadoopClusterHost(String name) throws ShuttlMBeanException;

	/**
	 * Sets the namenode host of a hadoop cluster
	 * 
	 * @param name
	 *          Name of the cluster
	 * @param host
	 *          Namenode host name
	 * @throws ShuttlMBeanException
	 */
	public void setHadoopClusterHost(String name, String host)
			throws ShuttlMBeanException;

	/**
	 * 
	 * @param name
	 *          The name of the cluster
	 * @return String HDFS cluster host name (namenode)
	 */
	public int getHadoopClusterPort(String name) throws ShuttlMBeanException;

	/**
	 * Sets the namenode port of a hadoop cluster
	 * 
	 * @param name
	 *          Name of the cluster
	 * @param port
	 *          Namenode port
	 * @throws ShuttlMBeanException
	 */
	public void setHadoopClusterPort(String name, int port)
			throws ShuttlMBeanException;

	/**
	 * 
	 * @return String HTTP/REST Listener Host
	 */
	public String getHttpHost();

	/**
	 * Sets the Network endpoint/hostname for the REST endpoints on this server
	 * 
	 * @param host
	 *          String
	 */
	public void setHttpHost(String host);

	/**
	 * Gets the HTTP port of the REST endpoints on this server
	 * 
	 * @return int HTTP/REST Listener port
	 */
	public int getHttpPort();

	/**
	 * Sets the HTTP port of the REST endpoints on this server
	 * 
	 * @param port
	 *          HTTP port
	 * @return
	 */
	public void setHttpPort(int port);

	/**
	 * Gets the name of the Splunk cluster to which it belongs
	 * 
	 * @return
	 */
	public String getSplunkClusterName();

	/**
	 * Sets the name of the Splunk cluster to which it belongs
	 * 
	 * @return
	 */
	public void setSplunkClusterName(String name);

	/**
	 * Saves the MBean state into an xml file
	 * 
	 */
	public void save() throws ShuttlMBeanException;

	/**
	 * Refreshes the MBeans with values from XML file
	 */
	public void refresh() throws ShuttlMBeanException;

	public ServerConf getServerConf();

	public void setServerConf(ServerConf conf);
}
