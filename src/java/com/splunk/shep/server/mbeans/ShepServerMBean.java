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

import com.splunk.shep.server.model.ServerConf;
/**
 * 
 * @author kpakkirisamy
 * 
 */
public interface ShepServerMBean {

    /**
     * 
     * @return String The default HDFS cluster host name
     */
    public String getDefHadoopClusterHost() throws ShepMBeanException;

    /**
     * 
     * @return String The default HDFS cluster port
     */
    public int getDefHadoopClusterPort() throws ShepMBeanException;

    /**
     * Name of the host on which Shep is running
     * 
     * @return
     * @throws ShepMBeanException
     */
    public String getShepHostName() throws ShepMBeanException;

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
     * @throws ShepMBeanException
     */
    public boolean isDefault(String clustername) throws ShepMBeanException;

    /**
     * Sets a cluster as the default for Shep to work with
     * 
     * @param clustername
     * @throws ShepMBeanException
     */
    public void setDefault(String clustername) throws ShepMBeanException;

    /**
     * 
     * @param name
     *            The name of the cluster
     * @return String HDFS cluster host name (namenode)
     */
    public String getHadoopClusterHost(String name) throws ShepMBeanException;

    /**
     * Sets the namenode host of a hadoop cluster
     * 
     * @param name
     *            Name of the cluster
     * @param host
     *            Namenode host name
     * @throws ShepMBeanException
     */
    public void setHadoopClusterHost(String name, String host)
	    throws ShepMBeanException;

    /**
     * 
     * @param name
     *            The name of the cluster
     * @return String HDFS cluster host name (namenode)
     */
    public int getHadoopClusterPort(String name) throws ShepMBeanException;

    /**
     * Sets the namenode port of a hadoop cluster
     * 
     * @param name
     *            Name of the cluster
     * @param port
     *            Namenode port
     * @throws ShepMBeanException
     */
    public void setHadoopClusterPort(String name, int port)
	    throws ShepMBeanException;

    /**
     * 
     * @return String HTTP/REST Listener Host
     */
    public String getHttpHost();

    /**
     * Sets the Network endpoint/hostname for the REST endpoints on this Shep
     * server
     * 
     * @param host
     *            String
     */
    public void setHttpHost(String host);

    /**
     * Gets the HTTP port of the REST endpoints on this Shep server
     * 
     * @return int HTTP/REST Listener port
     */
    public int getHttpPort();

    /**
     * Sets the HTTP port of the REST endpoints on this Shep server
     * 
     * @param port
     *            HTTP port
     * @return
     */
    public void setHttpPort(int port);

    /**
     * Gets the name of the Splunk cluster to which this Shep belongs
     * 
     * @return
     */
    public String getSplunkClusterName();

    /**
     * Sets the name of the Splunk cluster to which this Shep belongs
     * 
     * @return
     */
    public void setSplunkClusterName(String name);

    /**
     * Saves the MBean state into an xml file
     * 
     */
    public void save() throws ShepMBeanException;

    /**
     * Refreshes the MBeans with values from XML file
     */
    public void refresh() throws ShepMBeanException;

    public ServerConf getServerConf();

    public void setServerConf(ServerConf conf);
}
