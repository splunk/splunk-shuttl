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
package com.splunk.shuttl.archiver.archive;

import com.splunk.shuttl.server.mbeans.ShuttlArchiver;
import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;
import com.splunk.shuttl.server.mbeans.ShuttlMBeanException;
import com.splunk.shuttl.server.mbeans.util.RegistersMBeans;

/**
 * Registers the Archiver MBean. The class isolates the logic for register this
 * specific MBean. It also makes the BucketFreezer easier to test.
 */
public class RegistersArchiverMBean {
	private static final String archiverObjectName = ShuttlArchiverMBean.OBJECT_NAME;

	private RegistersMBeans registersMBeans;
	private ShuttlArchiverMBean archiverMBeanInstance;

	public RegistersArchiverMBean(RegistersMBeans registersMBeans,
			ShuttlArchiverMBean shuttlArchiverMBean) {
		this.registersMBeans = registersMBeans;
		this.archiverMBeanInstance = shuttlArchiverMBean;
	}

	public String getName() {
		return archiverObjectName;
	}

	/**
	 * Registers the Archiver mBean.
	 * 
	 * @throws ShuttlMBeanException
	 */
	public void register() throws ShuttlMBeanException {
		registersMBeans.registerMBean(archiverObjectName, archiverMBeanInstance);
	}

	/**
	 * Unregisters the Archiver mBean
	 */
	public void unregister() {
		registersMBeans.unregisterMBean(archiverObjectName);
	}

	public static RegistersArchiverMBean create() {
		return new RegistersArchiverMBean(RegistersMBeans.create(),
				new ShuttlArchiver());
	}
}
