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
package com.splunk.shuttl.server.mbeans.util;

import static org.testng.AssertJUnit.*;

import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.server.mbeans.ShuttlArchiver;
import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;
import com.splunk.shuttl.testutil.TUtilsConf;

@Test(groups = { "fast-unit" })
public class RegistersMBeansTest {

	ObjectName objectName;
	String objectNameString;
	MBeanServer mbs;
	Class<ShuttlArchiverMBean> interfaceClass;
	ShuttlArchiverMBean instance;

	RegistersMBeans registersMBeans;

	@BeforeMethod
	public void setUp() throws Exception {
		mbs = ManagementFactory.getPlatformMBeanServer();
		objectNameString = ShuttlArchiverMBean.OBJECT_NAME;
		objectName = new ObjectName(objectNameString);
		interfaceClass = ShuttlArchiverMBean.class;
		instance = ShuttlArchiver.createWithConfDirectory(TUtilsConf
				.getNullConfsDir());

		registersMBeans = new RegistersMBeans(mbs);
	}

	@AfterMethod
	public void tearDown() throws MBeanRegistrationException,
			InstanceNotFoundException {
		if (mbs.isRegistered(objectName))
			mbs.unregisterMBean(objectName);
		assertFalse(mbs.isRegistered(objectName));
	}

	@Test(groups = { "fast-unit" })
	public void registerMBean_notRegisteredMBean_registersMBean()
			throws Exception {
		assertFalse(mbs.isRegistered(objectName));

		registersMBeans.registerMBean(objectNameString, instance);
		assertTrue(mbs.isRegistered(objectName));
	}

	public void registerMBean_registeredMBean_doesNothing() throws Exception {
		registersMBeans.registerMBean(objectNameString, instance);
		assertTrue(mbs.isRegistered(objectName));

		registersMBeans.registerMBean(objectNameString, instance);
	}

	public void getMBeanInstance_registeredMBean_getsInstance() throws Exception {
		registersMBeans.registerMBean(objectNameString, instance);
		ShuttlArchiverMBean instance = MBeanUtils.getMBeanInstance(
				objectNameString, interfaceClass);
		assertNotNull(instance);
	}

	@Test(expectedExceptions = { InstanceNotFoundException.class })
	public void getMBeanInstance_notRegisteredInstance_throwInstanceNotFoundException()
			throws Exception {
		assertFalse(mbs.isRegistered(objectName));
		MBeanUtils.getMBeanInstance(objectNameString, interfaceClass);
	}

	@Test(expectedExceptions = { RuntimeException.class })
	public void getMBeanInstance_invalidObjectName_throwRuntimeException()
			throws InstanceNotFoundException {
		MBeanUtils.getMBeanInstance("fiskDirskrsko=-", interfaceClass);
	}

}
