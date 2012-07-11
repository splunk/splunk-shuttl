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

import static com.splunk.shuttl.testutil.TUtilsFile.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.splunk.shuttl.server.mbeans.JMXSplunkForTests;
import com.splunk.shuttl.server.mbeans.JMXSplunkMBean;
import com.splunk.shuttl.server.mbeans.ShuttlArchiverForTests;
import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;
import com.splunk.shuttl.server.mbeans.util.RegistersMBeans;

public class TUtilsMBean {

	public static final String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";

	/**
	 * Registers the Archiver and Splunk MBeans. Make sure to unregister after
	 * your test is run. <br/>
	 * Use {@link TUtilsMBean#runWithRegisteredMBeans(Runnable)} if possible.
	 */
	static void registerMBeans() {
		registerByNameAndClass(ShuttlArchiverMBean.OBJECT_NAME,
				ShuttlArchiverForTests.class);
		registerByNameAndClass(JMXSplunkMBean.OBJECT_NAME, JMXSplunkForTests.class);
	}

	private static void registerByNameAndClass(String objectName, Class<?> clazz) {
		try {
			RegistersMBeans.create().registerMBean(objectName, clazz);
		} catch (Exception e) {
			TUtilsTestNG
					.failForException("Could not register ShuttlArchiverMBean", e);
		}
	}

	/**
	 * Unregisters the ShuttlArchiverMBean
	 */
	static void unregisterMBeans() {
		RegistersMBeans.create().unregisterMBean(ShuttlArchiverMBean.OBJECT_NAME);
	}

	/**
	 * Runs a runnable while MBeans in {@link TUtilsMBean#registerMBeans()} are
	 * registered. The method makes sure that the MBeans are unregistered when the
	 * runnable has finished running.
	 */
	public static void runWithRegisteredMBeans(Runnable runnable) {
		try {
			registerMBeans();
			runnable.run();
		} finally {
			unregisterMBeans();
		}
	}

	/**
	 * Creates an empty conf file with a specified namespace. It's used for
	 * testing mbeans.
	 */
	public static File createEmptyInNamespace(String namespace) {
		File confFile = createFile();
		writeXmlBoilerPlateWithNamespaceToFile(namespace, confFile);
		return confFile;
	}

	private static void writeXmlBoilerPlateWithNamespaceToFile(String namespace,
			File confFile) {
		try {
			FileUtils.writeStringToFile(confFile, TUtilsMBean.XML_HEADER + "<ns2:"
					+ namespace + " xmlns:ns2=\"com.splunk.shuttl.server.model\">\n"
					+ "</ns2:" + namespace + ">\n");
		} catch (IOException e) {
			TUtilsTestNG.failForException("Could not write contents to file", e);
		}
	}
}
