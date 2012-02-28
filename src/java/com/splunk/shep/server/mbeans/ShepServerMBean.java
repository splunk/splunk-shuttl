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
     * 
     * @param name
     *            The name of the cluster
     * @return String HDFS cluster host name (namenode)
     */
    public String getHadoopClusterHost(String name);

    /**
     * 
     * @param name
     *            The name of the cluster
     * @return String HDFS cluster host name (namenode)
     */
    public int getHadoopClusterPort(String name);

    /**
     * 
     * @return String HTTP/REST Listener Host
     */
    public String getHttpHost();

    /**
     * 
     * @return int HTTP/REST Listener port
     */
    public int getHttpPort();
}
