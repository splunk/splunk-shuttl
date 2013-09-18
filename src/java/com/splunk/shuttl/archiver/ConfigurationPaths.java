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
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Paths to shuttl configuration files.
 */
public class ConfigurationPaths {

	public interface ShuttlHomeProvider {
		File getShuttlHome();
	}

	private static final String CONFIGURATION_PATH = "conf/";
	private static final String BACKEND_PROPERTIES_PATH = "conf/backend";
	private final ShuttlHomeProvider shuttlHomeProvider;

	public ConfigurationPaths(ShuttlHomeProvider shuttlHomeProvider) {
		this.shuttlHomeProvider = shuttlHomeProvider;
	}

	/**
	 * @return directory where Shuttl has it's MBean configuration files.
	 */
	public File getDefaultConfDirectory() {
		return new File(shuttlHomeProvider.getShuttlHome(), CONFIGURATION_PATH);
	}

	public File getBackendConfigDirectory() {
		return new File(shuttlHomeProvider.getShuttlHome(), BACKEND_PROPERTIES_PATH);
	}

	public static ConfigurationPaths create() {
		return new ConfigurationPaths(new ShuttlHomeProvider() {

			@Override
			public File getShuttlHome() {
				if (getShuttlHomeThroughJar() != null)
					return getShuttlHomeThroughJar();
				else if (getShuttlHomeThroughSplunkHome() != null)
					return getShuttlHomeThroughSplunkHome();
				else
					throw new RuntimeException(
							"Could not resolve Shuttl's install location");
			}

			private File getShuttlHomeThroughJar() {
				File shuttlJar = getJarLocation();
				File binDir = shuttlJar.getParentFile();
				File shuttlHome = binDir.getParentFile();
				return shuttlJar.getName().endsWith(".jar")
						&& binDir.getName().equals("bin")
						&& shuttlHome.getName().equals("shuttl") ? shuttlHome : null;
			}

			/**
			 * Taken from Stack overflow:
			 * 
			 * <pre>
			 * http://stackoverflow.com/questions/320542/how-to-get-the-path-of-a-running-jar-file
			 * </pre>
			 */
			private File getJarLocation() {
				try {
					return new File(URLDecoder.decode(ConfigurationPaths.class
							.getProtectionDomain().getCodeSource().getLocation().getPath(),
							"UTF-8"));
				} catch (UnsupportedEncodingException e) {
					return new File("/did/not/find/jar");
				}
			}

			private File getShuttlHomeThroughSplunkHome() {
				String splunkHome = System.getenv("SPLUNK_HOME");
				if (splunkHome == null)
					return null;

				File shuttlThroughSplunkHome = new File(splunkHome, "etc/apps/shuttl");
				if (!shuttlThroughSplunkHome.exists())
					return null;

				return shuttlThroughSplunkHome;
			}

		});
	}
}
