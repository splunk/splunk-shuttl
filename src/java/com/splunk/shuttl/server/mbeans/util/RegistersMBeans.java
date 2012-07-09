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

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketFreezer;
import com.splunk.shuttl.server.mbeans.ShuttlMBeanException;

/**
 * Registers MBeans in code instead of registering them with an xml file. <br/>
 * <br/>
 * The Server contains all the configuration and it is conf'ed with xml. The
 * {@link BucketFreezer} is not run on the Server and doesn't have any
 * configuration. It needs to manually register any classes that should
 * represent the configuration.
 */
public class RegistersMBeans {

	private static Logger logger = Logger.getLogger(RegistersMBeans.class);

	/**
	 * Registers an MBean
	 * 
	 * @param name
	 *          the name of the MBean to register.
	 * @param clazz
	 *          the MBean class to use.
	 * @throws ShuttlMBeanException
	 *           when it was not possible to register the name to that class.
	 */
	public void registerMBean(String name, Class<?> clazz)
			throws ShuttlMBeanException {
		try {
			MBeanUtils.registerMBean(name, clazz);
		} catch (Exception e) {
			logger.error(did("Tried registering MBean by name and class", e,
					"To register the MBean", "name", name, "class", clazz));
			throw new ShuttlMBeanException(e);
		}
	}

	/**
	 * Unregisters an MBean
	 * 
	 * @param name
	 *          to unregister
	 * @throws Exception
	 */
	public void unregisterMBean(String name) {
		try {
			MBeanUtils.unregisterMBean(name);
		} catch (Exception e) {
			logger.debug(warn("Unregistered MBean by name", e,
					"Will swallow exception and" + " assume everything is fine.", "name",
					name));
		}
	}
}
