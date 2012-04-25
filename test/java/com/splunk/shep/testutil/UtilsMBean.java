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
package com.splunk.shep.testutil;

import javax.management.InstanceAlreadyExistsException;

import com.splunk.shep.server.mbeans.ShepArchiverForTests;
import com.splunk.shep.server.mbeans.ShepArchiverMBean;
import com.splunk.shep.server.mbeans.util.MBeanUtils;

public class UtilsMBean {

    /**
     * Registers the ShepArchiverMBean
     */
    public static void registerShepArchiverMBean() {
	try {
	    MBeanUtils.registerMBean(ShepArchiverMBean.OBJECT_NAME,
		    ShepArchiverForTests.class);
	} catch (InstanceAlreadyExistsException e1) {
	    // Ok.
	} catch (Exception e) {
	    UtilsTestNG.failForException(
		    "Could not register ShepArchiverMBean", e);
	}
    }
}
