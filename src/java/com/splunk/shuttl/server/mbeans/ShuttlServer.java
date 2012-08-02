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

import com.splunk.shuttl.server.model.ServerConf;

/**
 * 
 * @author kpakkirisamy
 * 
 */
public class ShuttlServer extends MBeanBase<ServerConf> implements
		ShuttlServerMBean {

	private ServerConf conf;

	public ShuttlServer() {
		super();
	}

	public ShuttlServer(File confDirectory) {
		super(confDirectory);
	}

	/**
	 * Used for different kinds of test, and the String cannot be of type File,
	 * because the File constructor is already taken.
	 */
	public ShuttlServer(String confFilePath) {
		super(confFilePath);
	}

	@Override
	protected String getDefaultConfFileName() {
		return "server.xml";
	}

	@Override
	public String getHttpHost() {
		return this.conf.getHttpHost();
	}

	@Override
	public void setHttpHost(String host) {
		this.conf.setHttpHost(host);
	}

	@Override
	public int getHttpPort() {
		return this.conf.getHttpPort();
	}

	@Override
	public void setHttpPort(int port) {
		this.conf.setHttpPort(port);
	}

	@Override
	protected ServerConf getConfObject() {
		return conf;
	}

	@Override
	protected Class<ServerConf> getConfClass() {
		return ServerConf.class;
	}

	@Override
	protected void setConfObject(ServerConf conf) {
		this.conf = conf;
	}
}
