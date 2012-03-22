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
 * All config/management attributes/operations of Forwarder
 * 
 * @author kpakkirisamy
 * 
 */
public interface ShepForwarderMBean {

    /**
     * Retrieves the HDFSSinkPrefix attribute
     * 
     * @param name
     *            The name of the cluster as in server.xml
     * @return The HDFSSinkPrefix attribute
     */
    public String getHDFSSinkPrefix(String name) throws ShepMBeanException;

    /**
     * Retrieves the HDFSSinkMaxEventSize
     * 
     * @param name
     *            The name of the cluster as in server.xml
     * 
     * @return The HDFSSinkMaxEventSize
     */
    public long getHDFSSinkFileRollingSize(String name)
	    throws ShepMBeanException;

    /**
     * Retrieves the HDFSSinkPrefix attribute
     * 
     * @param name
     *            The name of the cluster as in server.xml
     * @return The HDFSSinkPrefix attribute
     */
    public boolean getHDFSSinkUseAppending(String name)
	    throws ShepMBeanException;

    /**
     * Starts the SplunkExportService
     * 
     * @throws ShepMBeanException
     */
    public void startExportService() throws ShepMBeanException;

    /**
     * Stops the SplunkExportService
     * 
     * @throws ShepMBeanException
     */
    public void stopExportService() throws ShepMBeanException;

    /**
     * Gets the SplunkExportService status message
     * 
     * @return status The status message
     * @throws ShepMBeanException
     */
    public String getExportServiceStatus() throws ShepMBeanException;
}
