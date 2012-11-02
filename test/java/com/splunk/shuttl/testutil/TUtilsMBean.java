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

import com.splunk.shuttl.server.mbeans.JMXSplunk;
import com.splunk.shuttl.server.mbeans.JMXSplunkMBean;
import com.splunk.shuttl.server.mbeans.ShuttlArchiver;
import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;
import com.splunk.shuttl.server.mbeans.ShuttlServer;
import com.splunk.shuttl.server.mbeans.util.RegistersMBeans;

public class TUtilsMBean {

	public static final String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";

	/**
	 * Registers the Archiver and Splunk MBeans. Make sure to unregister after
	 * your test is run. <br/>
	 * Use {@link TUtilsMBean#runWithRegisteredMBeans(Runnable)} if possible.
	 */
	static void registerMBeans(File confDir) {
		registerByNameAndClass(ShuttlArchiverMBean.OBJECT_NAME,
				ShuttlArchiver.createWithConfDirectory(confDir));
		registerByNameAndClass(JMXSplunkMBean.OBJECT_NAME,
				JMXSplunk.createWithConfDirectory(confDir));
		registerByNameAndClass(ShuttlServer.OBJECT_NAME, new ShuttlServer(confDir));
	}

	private static void registerByNameAndClass(String objectName, Object mBean) {
		try {
			RegistersMBeans.create().registerMBean(objectName, mBean);
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
	 * 
	 * @param path
	 *          to the directory where configured Shuttl confs live.
	 */
	public static void runWithRegisteredMBeans(File confDir, Runnable runnable) {
		try {
			registerMBeans(confDir);
			runnable.run();
		} finally {
			unregisterMBeans();
		}
	}

	/**
	 * Creates an empty conf file with a specified namespace. It's used for
	 * testing mbeans.
	 */
	public static File createEmptyConfInNamespace(String namespace) {
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
