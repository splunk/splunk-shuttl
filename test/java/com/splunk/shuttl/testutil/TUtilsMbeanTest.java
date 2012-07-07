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

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import javax.management.InstanceNotFoundException;
import javax.management.OperationsException;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;
import com.splunk.shuttl.server.mbeans.util.MBeanUtils;

@Test(groups = { "fast-unit" })
public class TUtilsMbeanTest {

	@BeforeMethod
	public void setUp() {
		TUtilsMBean.unregisterShuttlArchiverMBean();
	}

	@AfterMethod
	public void tearDown() {
		TUtilsMBean.unregisterShuttlArchiverMBean();
	}

	@Test(groups = { "fast-unit" })
	public void registerShuttlArchiverMBean_notRegistered_registersMbean()
			throws InstanceNotFoundException {
		assertFalse(isMBeanRegistered());
		TUtilsMBean.registerShuttlArchiverMBean();
		assertTrue(isMBeanRegistered());
		TUtilsMBean.unregisterShuttlArchiverMBean();
		assertFalse(isMBeanRegistered());
	}

	private boolean isMBeanRegistered() throws InstanceNotFoundException {
		try {
			ShuttlArchiverMBean proxy = MBeanUtils.getMBeanInstance(
					ShuttlArchiverMBean.OBJECT_NAME, ShuttlArchiverMBean.class);
			return proxy != null;
		} catch (InstanceNotFoundException e) {
			return false;
		}
	}

	public void registerShuttlArchiverMBean_twice_ok() {
		TUtilsMBean.registerShuttlArchiverMBean();
		TUtilsMBean.registerShuttlArchiverMBean();
	}

	public void unregisterShuttlArchiverMBean_twice_ok() {
		TUtilsMBean.unregisterShuttlArchiverMBean();
		TUtilsMBean.unregisterShuttlArchiverMBean();
	}

	public void runWithRegisteredShuttlArchiverMBean_withRunnable_runsRunnable() {
		Runnable runnable = mock(Runnable.class);
		TUtilsMBean.runWithRegisteredShuttlArchiverMBean(runnable);
		verify(runnable).run();
	}

	public void runWithRegisteredShuttlArchiverMBean_withRunnable_mBeanIsRegistered()
			throws OperationsException {
		assertFalse(isMBeanRegistered());
		TUtilsMBean.runWithRegisteredShuttlArchiverMBean(new Runnable() {
			@Override
			public void run() {
				try {
					assertTrue(isMBeanRegistered());
				} catch (InstanceNotFoundException e) {
					TUtilsTestNG.failForException(null, e);
				}
			}
		});
	}

	public void runWithRegisteredShuttlArchiverMBean_throwsExceptionInRunnable_stillUnregisteredAfterRun()
			throws InstanceNotFoundException {
		RuntimeException expectedException = null;
		try {
			TUtilsMBean.runWithRegisteredShuttlArchiverMBean(new Runnable() {
				@Override
				public void run() {
					throw new RuntimeException();
				}
			});
		} catch (RuntimeException e) {
			expectedException = e;
		}
		assertFalse(isMBeanRegistered());
		assertNotNull(expectedException);
	}
}
