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

import com.splunk.shep.server.model.ArchiverConfInterface;

/**
 * @author kpakkirisamy
 *
 */
public interface ShepArchiverMBean extends ArchiverConfInterface {
    /**
     * Saves MBean attributes to backing xml
     * 
     * @throws ShepMBeanException
     */
    public void save() throws ShepMBeanException;

    /**
     * Reloads MBean attributes from backing xml
     * 
     * @throws ShepMBeanException
     */
    public void refresh() throws ShepMBeanException;

    /**
     * Adds an index to be archived
     * 
     * @param name
     */
    public void addIndex(String name);

    /**
     * deletes an index from being archived
     * 
     * @param name
     */
    public void deleteIndex(String name);

}
