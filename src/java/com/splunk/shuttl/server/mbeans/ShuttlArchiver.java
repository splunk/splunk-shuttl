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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.util.List;

import javax.management.InstanceNotFoundException;

import org.apache.log4j.Logger;

import com.splunk.shuttl.server.mbeans.util.MBeanUtils;
import com.splunk.shuttl.server.model.ArchiverConf;

/**
 * @author kpakkirisamy
 */
public class ShuttlArchiver extends MBeanBase<ArchiverConf> implements
		ShuttlArchiverMBean {

	private static Logger logger = Logger.getLogger(ShuttlArchiver.class);
	private ArchiverConf conf;
	private String xmlFilePath;

	public ShuttlArchiver() {
		this.xmlFilePath = getPathToDefaultConfFile();
		refreshWithConf();
	}

	@Override
	protected String getConfFileName() {
		return "archiver.xml";
	}

	// used by tests
	public ShuttlArchiver(String confFilePath) throws ShuttlMBeanException {
		this.xmlFilePath = confFilePath;
		refreshWithConf();
	}

	private void refreshWithConf() {
		try {
			refresh();
		} catch (Exception e) {
			logger.error(did("Tried refreshing " + getClass().getSimpleName()
					+ " with conf file.", e, "To get new values from the conf file.",
					"mBean", this, "exception", e));
			throw new RuntimeException(e);
		}
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
	public String getArchiverRootURI() {
		return conf.getArchiverRootURI();
	}

	@Override
	public void setArchiverRootURI(String uri) {
		conf.setArchiverRootURI(uri);
	}

	@Override
	protected String getPathToXmlFile() {
		return this.xmlFilePath;
	}

	@Override
	protected ArchiverConf getConfObject() {
		return this.conf;
	}

	@Override
	protected void setConfObject(ArchiverConf conf) {
		this.conf = conf;
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

}
