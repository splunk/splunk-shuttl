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
package com.splunk.shuttl.archiver;

import java.io.File;

/**
 * Paths to shuttl configuration files.
 */
public class ConfigurationPaths {

	private static final String CONFIGURATION_PATH = "etc/apps/shuttl/conf/";
	private static final String BACKEND_PROPERTIES_PATH = "etc/apps/shuttl/conf/backend";

	/**
	 * @return directory where shuttl has it's MBean configuration files.
	 */
	public static File getDefaultConfDirectory() {
		return new File(getSplunkHome(), CONFIGURATION_PATH);
	}

	public static File getBackendConfigDirectory() {
		return new File(getSplunkHome(), BACKEND_PROPERTIES_PATH);
	}

	private static String getSplunkHome() {
		String splunkHome = System.getenv("SPLUNK_HOME");
		if (splunkHome == null)
			throw new RuntimeException("SPLUNK_HOME was not set.");
		return splunkHome;
	}
}
