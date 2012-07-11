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
package com.splunk.shuttl.archiver.thaw;

import com.splunk.shuttl.server.mbeans.SplunkMBean;

/**
 * Configuration for getting Splunk host, post, username, password to the
 * configured Splunk instance.
 */
public class SplunkConfiguration {

	private final String host;
	private final int port;
	private final String username;
	private final String password;

	public SplunkConfiguration(String host, int port, String username,
			String password) {
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
	}

	/**
	 * @return host to the configured Splunk instance.
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @return port to the configured Splunk instance.
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @return username to the configured Splunk instance.
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @return to the configured Splunk instance.
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @return instance with default configuration.
	 */
	public static SplunkConfiguration create() {
		return new SplunkConfiguration("localhost", 8089, "admin", "changeme");
	}

	/**
	 * @param mBean
	 *          to create the instance from.
	 */
	public static SplunkConfiguration createWithMBean(SplunkMBean mBean) {
		return new SplunkConfiguration(mBean.getHost(), getPortFromMBean(mBean),
				mBean.getUsername(), mBean.getPassword());
	}

	private static int getPortFromMBean(SplunkMBean mBean) {
		if (mBean.getPort() != null)
			return Integer.parseInt(mBean.getPort());
		return -1;
	}

}
