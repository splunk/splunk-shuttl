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

import com.splunk.shep.server.model.ExporterConfiguration;

/**
 * @author kpakkirisamy
 *
 */
public interface ShepExporterMBean extends ExporterConfiguration {
    /**
     * Starts Exporter service
     * 
     * @throws ShepMBeanException
     */
    public void startExporterService() throws ShepMBeanException;

    /**
     * Start Exporter service
     * 
     * @throws ShepMBeanException
     */
    public void stopExporterService() throws ShepMBeanException;

    /**
     * Saves configuration and state information
     * 
     */
    public void save() throws ShepMBeanException;

    /**
     * Reloads configuration and state information
     * 
     * @throws ShepMBeanException
     */
    public void refresh() throws ShepMBeanException;

    /**
     * Gets the status of the service
     * 
     * @return
     */
    public String getStatus();

}
