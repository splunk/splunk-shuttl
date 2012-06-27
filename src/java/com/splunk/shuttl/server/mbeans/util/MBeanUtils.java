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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

public class MBeanUtils {

	private static final MBeanServer mbs;

	static {
		mbs = ManagementFactory.getPlatformMBeanServer();
	}

	/**
	 * Registers an MBean
	 * 
	 * @param objectName
	 *          the name of the MBean to register.
	 * @param clazz
	 *          the MBean class to use.
	 * @throws Exception
	 */
	public static void registerMBean(String name, Class<?> clazz)
			throws Exception {
		ObjectName objectName = new ObjectName(name);
		if (!mbs.isRegistered(objectName))
			mbs.registerMBean(clazz.newInstance(), objectName);
	}

	/**
	 * Retrieves an instance of a specific MBean
	 * 
	 * @param objectName
	 *          The object name
	 * @param clazz
	 *          Reference to the class wanted
	 * @return If the object name and class are correct, a reference to an
	 *         instance of the class
	 * @throws InstanceNotFoundException
	 */
	public static <T> T getMBeanInstance(String name, Class<T> clazz)
			throws InstanceNotFoundException {
		ObjectName objectName = getObjectNameWithErrorHandling(name, clazz);

		// force exception if mbean is unregistred
		mbs.getObjectInstance(objectName);
		return clazz.cast(MBeanServerInvocationHandler.newProxyInstance(mbs,
				objectName, clazz, false));
	}

	private static <T> ObjectName getObjectNameWithErrorHandling(String name,
			Class<T> clazz) {
		ObjectName objectName = null;
		try {
			objectName = new ObjectName(name);
		} catch (Exception e) {
			logException(name, clazz, e);
			throw new RuntimeException(e);
		}
		return objectName;
	}

	private static <T> void logException(String name, Class<T> clazz, Exception e) {
		Logger.getLogger(MBeanUtils.class).debug(
				did("Tried creating ObjectName for MBean name: " + name, e,
						"To create ObjectName", "mbean_name", name, "class" + clazz));
	}
}
