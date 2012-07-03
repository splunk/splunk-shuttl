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

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.splunk.shuttl.server.mbeans.util.JAXBUtils;
import com.splunk.shuttl.server.mbeans.util.MBeanUtils;
import com.splunk.shuttl.server.model.ArchiverConf;

/**
 * @author kpakkirisamy
 */
public class ShuttlArchiver implements ShuttlArchiverMBean {
	// error messages
	private static final String SHUTTL_ARCHIVER_INIT_FAILURE = "ShuttlArchiver init failure";
	// end error messages

	private static String ARCHIVERCONF_XML = "etc/apps/shuttl/conf/archiver.xml";
	private static Logger logger = Logger.getLogger(ShuttlArchiver.class);
	private ArchiverConf conf;
	private String xmlFilePath;

	public ShuttlArchiver() throws ShuttlMBeanException {
		try {
			this.xmlFilePath = getArchiverConfXml();
			refresh();
		} catch (Exception e) {
			logger.error(SHUTTL_ARCHIVER_INIT_FAILURE, e);
			throw new ShuttlMBeanException(e);
		}
	}

	protected String getArchiverConfXml() {
		return System.getProperty("splunk.home") + ARCHIVERCONF_XML;
	}

	// used by tests
	public ShuttlArchiver(String confFilePath) throws ShuttlMBeanException {
		try {
			this.xmlFilePath = confFilePath;
			refresh();
		} catch (Exception e) {
			logger.error(SHUTTL_ARCHIVER_INIT_FAILURE, e);
			throw new ShuttlMBeanException(e);
		}
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
	public List<String> getIndexNames() {
		return conf.getIndexNames();
	}

	@Override
	public void setIndexNames(List<String> indexNames) {
		conf.setIndexNames(indexNames);
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
	public String getTmpDirectory() {
		return conf.getTmpDirectory();
	}

	@Override
	public void setTmpDirectory(String path) {
		conf.setTmpDirectory(path);
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
	public void addIndex(String name) {
		if (conf.getIndexNames() == null)
			conf.setIndexNames(new ArrayList<String>());
		conf.getIndexNames().add(name);
	}

	@Override
	public void deleteIndex(String name) {
		conf.getIndexNames().remove(name);
	}

	@Override
	public void save() throws ShuttlMBeanException { // TODO move to util class
		try {
			JAXBUtils.save(ArchiverConf.class, this.conf, this.xmlFilePath);
		} catch (Exception e) {
			logger.error(e);
			throw new ShuttlMBeanException(e);
		}
	}

	@Override
	public void refresh() throws ShuttlMBeanException { // TODO move to util
		// class
		try {
			this.conf = (ArchiverConf) JAXBUtils.refresh(ArchiverConf.class,
					this.xmlFilePath);
		} catch (FileNotFoundException fnfe) {
			this.conf = new ArchiverConf();
		} catch (Exception e) {
			logger.error(e);
			throw new ShuttlMBeanException(e);
		}
	}

	/**
	 * @return
	 */
	public static ShuttlArchiverMBean getMBeanProxy() {
		try {
			return MBeanUtils.getMBeanInstance(ShuttlArchiverMBean.OBJECT_NAME,
					ShuttlArchiverMBean.class);
		} catch (Exception e) {
			logger.error(e);
			throw new RuntimeException(e);
		}
	}
}
