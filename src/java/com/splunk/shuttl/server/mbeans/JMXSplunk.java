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
package com.splunk.shuttl.server.mbeans;

import java.io.File;

import javax.management.InstanceNotFoundException;

import com.splunk.shuttl.server.mbeans.util.MBeanUtils;
import com.splunk.shuttl.server.model.SplunkConf;

/**
 * Implementation of {@link JMXSplunkMBean}
 */
public class JMXSplunk extends MBeanBase<SplunkConf> implements JMXSplunkMBean {

	private SplunkConf conf;

	public JMXSplunk() {
		super();
	}

	private JMXSplunk(File confDirectory) {
		super(confDirectory);
	}

	private JMXSplunk(String confFilePath) {
		super(confFilePath);
	}

	@Override
	protected String getDefaultConfFileName() {
		return "splunk.xml";
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return conf.getHost();
	}

	/**
	 * @param host
	 *          the host to set
	 */
	public void setHost(String host) {
		conf.setHost(host);
	}

	/**
	 * @return the port
	 */
	public String getPort() {
		return conf.getPort();
	}

	/**
	 * @param port
	 *          the port to set
	 */
	public void setPort(String port) {
		conf.setPort(port);
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return conf.getUsername();
	}

	/**
	 * @param username
	 *          the username to set
	 */
	public void setUsername(String username) {
		conf.setUsername(username);
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return conf.getPassword();
	}

	/**
	 * @param password
	 *          the password to set
	 */
	public void setPassword(String password) {
		conf.setPassword(password);
	}

	@Override
	protected SplunkConf getConfObject() {
		return conf;
	}

	@Override
	protected void setConfObject(SplunkConf conf) {
		this.conf = conf;
	}

	@Override
	protected Class<SplunkConf> getConfClass() {
		return SplunkConf.class;
	}

	/**
	 * @return instance of {@link JMXSplunk}
	 * @throws InstanceNotFoundException
	 * @see {@link MBeanUtils#getMBeanInstance(String, Class)}
	 */
	public static JMXSplunkMBean getMBeanProxy() throws InstanceNotFoundException {
		return MBeanUtils.getMBeanInstance(JMXSplunkMBean.OBJECT_NAME,
				JMXSplunkMBean.class);
	}

	public static JMXSplunk createWithConfDirectory(File confDirectory) {
		return new JMXSplunk(confDirectory);
	}

	public static JMXSplunk createWithConfFile(File confFile) {
		return new JMXSplunk(confFile.getAbsolutePath());
	}
}
