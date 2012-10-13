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
import java.util.List;

import javax.management.InstanceNotFoundException;

import com.splunk.shuttl.server.mbeans.util.MBeanUtils;
import com.splunk.shuttl.server.model.ArchiverConf;

/**
 * @author kpakkirisamy
 */
public class ShuttlArchiver extends MBeanBase<ArchiverConf> implements
		ShuttlArchiverMBean {

	private ArchiverConf conf;

	public ShuttlArchiver() {
		super();
	}

	private ShuttlArchiver(File confDirectory) {
		super(confDirectory);
	}

	/**
	 * Used for testing and needs to be a String instead of a File, because the
	 * File constructor is already taken.
	 */
	private ShuttlArchiver(String confFilePath) {
		super(confFilePath);
	}

	@Override
	protected String getDefaultConfFileName() {
		return "archiver.xml";
	}

	@Override
	public String getLocalArchiverDir() {
		return conf.getLocalArchiverDir();
	}

	@Override
	public void setLocalArchiverDir(String localArchiverDir) {
		conf.setLocalArchiverDir(localArchiverDir);
	}

	@Override
	public String getClusterName() {
		return conf.getClusterName();
	}

	@Override
	public void setClusterName(String clusterName) {
		conf.setClusterName(clusterName);
	}

	@Override
	public List<String> getArchiveFormats() {
		return conf.getArchiveFormats();
	}

	@Override
	public void setArchiveFormats(List<String> formats) {
		conf.setArchiveFormats(formats);
	}

	@Override
	public List<String> getBucketFormatPriority() {
		return conf.getBucketFormatPriority();
	}

	@Override
	public void setBucketFormatPriority(List<String> priorityList) {
		conf.setBucketFormatPriority(priorityList);
	}

	@Override
	public String getServerName() {
		return conf.getServerName();
	}

	@Override
	public void setServerName(String serverName) {
		conf.setServerName(serverName);
	}

	@Override
	public String getBackendName() {
		return conf.getBackendName();
	}

	@Override
	public void setBackendName(String backendName) {
		conf.setBackendName(backendName);
	}

	@Override
	public String getArchivePath() {
		return conf.getArchivePath();
	}

	@Override
	public void setArchivePath(String archivePath) {
		conf.setArchivePath(archivePath);
	}

	@Override
	protected ArchiverConf getConfObject() {
		return this.conf;
	}

	@Override
	protected void setConfObject(ArchiverConf conf) {
		this.conf = conf;
		if (conf.getArchiverRootURI() != null)
			new OverrideWithOldArchiverRootURIConfiguration(conf).override();
	}

	@Override
	protected Class<ArchiverConf> getConfClass() {
		return ArchiverConf.class;
	}

	/**
	 * @return instance of {@link ShuttlArchiverMBean}
	 * @throws InstanceNotFoundException
	 * @see {@link MBeanUtils#getMBeanInstance(String, Class)}
	 */
	public static ShuttlArchiverMBean getMBeanProxy()
			throws InstanceNotFoundException {
		return MBeanUtils.getMBeanInstance(ShuttlArchiverMBean.OBJECT_NAME,
				ShuttlArchiverMBean.class);
	}

	public static ShuttlArchiver createWithConfDirectory(File confDirectory) {
		return new ShuttlArchiver(confDirectory);
	}

	public static ShuttlArchiver createWithConfFile(File confFile) {
		return new ShuttlArchiver(confFile.getAbsolutePath());
	}

}
