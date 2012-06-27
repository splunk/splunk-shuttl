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

import static org.testng.Assert.*;

import javax.management.InstanceNotFoundException;

import org.testng.annotations.Test;

import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;
import com.splunk.shuttl.server.mbeans.util.MBeanUtils;

@Test(groups = { "fast-unit" })
public class TUtilsMbeanTest {

	@Test(groups = { "fast-unit" })
	public void registerShuttlArchiverMBean_notRegistered_registersMbean()
			throws InstanceNotFoundException {
		TUtilsMBean.registerShuttlArchiverMBean();
		ShuttlArchiverMBean proxy = MBeanUtils.getMBeanInstance(
				ShuttlArchiverMBean.OBJECT_NAME, ShuttlArchiverMBean.class);
		assertNotNull(proxy);
	}

	public void registerShuttlArchiverMBean_twice_ok() {
		TUtilsMBean.registerShuttlArchiverMBean();
		TUtilsMBean.registerShuttlArchiverMBean();
	}
}
