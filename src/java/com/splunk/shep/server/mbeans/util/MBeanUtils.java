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
package com.splunk.shep.server.mbeans.util;

import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

public class MBeanUtils {

    /**
     * Registers an MBean
     * 
     * @param objectName
     *            the name of the MBean to register.
     * @param clazz
     *            the MBean class to use.
     * @throws Exception
     */
    public static void registerMBean(String objectName, Class<?> clazz)
	    throws Exception {
	MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
	mbs.registerMBean(clazz.newInstance(), new ObjectName(objectName));
    }

    /**
     * Retrieves an instance of a specific MBean
     * 
     * @param objectName
     *            The object name
     * @param clazz
     *            Reference to the class wanted
     * @return If the object name and class are correct, a reference to an
     *         instance of the class
     * @throws InstanceNotFoundException
     */
    public static <T> T getMBeanInstance(String objectName, Class<T> clazz)
	    throws InstanceNotFoundException {
	MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
	ObjectName objname = null;
	try {
	    objname = new ObjectName(objectName);
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}

	// force exception if mbean is unregistred
	mbs.getObjectInstance(objname);

	return clazz.cast(MBeanServerInvocationHandler.newProxyInstance(mbs,
		objname, clazz, false));
    }
}
