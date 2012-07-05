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

/**
 * 
 * @author kpakkirisamy
 * 
 */
public interface ShuttlServerMBean {

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
	 * Saves the MBean state into an xml file
	 * 
	 */
	public void save() throws ShuttlMBeanException;

	/**
	 * Refreshes the MBeans with values from XML file
	 */
	public void refresh() throws ShuttlMBeanException;

}
