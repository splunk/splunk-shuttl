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

import java.io.File;

import javax.management.InstanceNotFoundException;
import javax.management.OperationsException;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.server.mbeans.JMXSplunkMBean;
import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;
import com.splunk.shuttl.server.mbeans.util.MBeanUtils;

@Test(groups = { "fast-unit" })
public class TUtilsMbeanTest {

	private File nullConfsDir;

	@BeforeMethod
	public void setUp() {
		nullConfsDir = TUtilsConf.getNullConfsDir();
		TUtilsMBean.unregisterMBeans();
	}

	@AfterMethod
	public void tearDown() {
		TUtilsMBean.unregisterMBeans();
	}

	@Test(groups = { "fast-unit" })
	public void registerMBeans_notRegistered_registersMBeans()
			throws InstanceNotFoundException {
		assertFalse(areMBeansRegistered());
		TUtilsMBean.registerMBeans(nullConfsDir);
		assertTrue(areMBeansRegistered());
		TUtilsMBean.unregisterMBeans();
		assertFalse(areMBeansRegistered());
	}

	private boolean areMBeansRegistered() throws InstanceNotFoundException {
		try {
			ShuttlArchiverMBean archiverMBean = MBeanUtils.getMBeanInstance(
					ShuttlArchiverMBean.OBJECT_NAME, ShuttlArchiverMBean.class);
			JMXSplunkMBean splunkMBean = MBeanUtils.getMBeanInstance(
					JMXSplunkMBean.OBJECT_NAME, JMXSplunkMBean.class);
			return archiverMBean != null && splunkMBean != null;
		} catch (InstanceNotFoundException e) {
			return false;
		}
	}

	public void registerMBeans_twice_ok() {
		TUtilsMBean.registerMBeans(nullConfsDir);
		TUtilsMBean.registerMBeans(nullConfsDir);
	}

	public void unregisterMBeans_twice_ok() {
		TUtilsMBean.unregisterMBeans();
		TUtilsMBean.unregisterMBeans();
	}

	public void runWithRegisteredMBeans_withRunnable_runsRunnable() {
		Runnable runnable = mock(Runnable.class);
		TUtilsMBean.runWithRegisteredMBeans(nullConfsDir, runnable);
		verify(runnable).run();
	}

	public void runWithRegisteredMBean_withRunnable_mBeanIsRegistered()
			throws OperationsException {
		assertFalse(areMBeansRegistered());
		TUtilsMBean.runWithRegisteredMBeans(nullConfsDir, new Runnable() {
			@Override
			public void run() {
				try {
					assertTrue(areMBeansRegistered());
				} catch (InstanceNotFoundException e) {
					TUtilsTestNG.failForException(null, e);
				}
			}
		});
	}

	public void runWithRegisteredMBean_throwsExceptionInRunnable_stillUnregisteredAfterRun()
			throws InstanceNotFoundException {
		RuntimeException expectedException = null;
		try {
			TUtilsMBean.runWithRegisteredMBeans(nullConfsDir, new Runnable() {
				@Override
				public void run() {
					throw new RuntimeException();
				}
			});
		} catch (RuntimeException e) {
			expectedException = e;
		}
		assertFalse(areMBeansRegistered());
		assertNotNull(expectedException);
	}
}
