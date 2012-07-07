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
package com.splunk.shuttl.testutil;

import javax.management.InstanceAlreadyExistsException;

import com.splunk.shuttl.server.mbeans.ShuttlArchiverForTests;
import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;
import com.splunk.shuttl.server.mbeans.util.MBeanUtils;

public class TUtilsMBean {

	/**
	 * Registers the ShuttlArchiverMBean. Make sure to unregister after your test
	 * is run. <br/>
	 * Use {@link TUtilsMBean#runWithRegisteredShuttlArchiverMBean(Runnable)} if
	 * possible.
	 */
	public static void registerShuttlArchiverMBean() {
		try {
			MBeanUtils.registerMBean(ShuttlArchiverMBean.OBJECT_NAME,
					ShuttlArchiverForTests.class);
		} catch (InstanceAlreadyExistsException e1) {
			// Ok.
		} catch (Exception e) {
			TUtilsTestNG
					.failForException("Could not register ShuttlArchiverMBean", e);
		}
	}

	/**
	 * Unregisters the ShuttlArchiverMBean
	 */
	public static void unregisterShuttlArchiverMBean() {
		try {
			MBeanUtils.unregisterMBean(ShuttlArchiverMBean.OBJECT_NAME);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void runWithRegisteredShuttlArchiverMBean(Runnable runnable) {
		try {
			registerShuttlArchiverMBean();
			runnable.run();
		} finally {
			unregisterShuttlArchiverMBean();
		}
	}
}
