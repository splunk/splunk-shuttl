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
package com.splunk.shep.server.mbeans;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.splunk.shep.server.mbeans.util.JAXBUtils;
import com.splunk.shep.server.model.ArchiverConf;


/**
 * @author kpakkirisamy
 */
public class ShepArchiver implements ShepArchiverMBean {
    // error messages
    private static final String SHEP_ARCHIVER_INIT_FAILURE = "ShepArchiver init failure";
    // end error messages

    private static String ARCHIVERCONF_XML = "etc/apps/shep/conf/archiver.xml";
    private Logger logger = Logger.getLogger(getClass());
    private ArchiverConf conf;
    private String xmlFilePath;

    public ShepArchiver() throws ShepMBeanException {
	try {
	    this.xmlFilePath = System.getProperty("splunk.home")
		    + ARCHIVERCONF_XML;
	    refresh();
	} catch (Exception e) {
	    logger.error(SHEP_ARCHIVER_INIT_FAILURE, e);
	    throw new ShepMBeanException(e);
	}
    }

    // used by tests
    public ShepArchiver(String confFilePath) throws ShepMBeanException {
	try {
	    this.xmlFilePath = confFilePath;
	    refresh();
	} catch (Exception e) {
	    logger.error(SHEP_ARCHIVER_INIT_FAILURE, e);
	    throw new ShepMBeanException(e);
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
    public String getArchiveFormat() {
	return conf.getArchiveFormat();
    }

    @Override
    public void setArchiveFormat(String format) {
	conf.setArchiveFormat(format);
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
	if (conf.getIndexNames() == null) {
	    conf.setIndexNames(new ArrayList<String>());
	}
	conf.getIndexNames().add(name);
    }

    @Override
    public void deleteIndex(String name) {
	conf.getIndexNames().remove(name);
    }

    @Override
    public void save() throws ShepMBeanException { // TODO move to util class
	try {
	    JAXBUtils.save(ArchiverConf.class, this.conf, this.xmlFilePath);
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepMBeanException(e);
	}
    }

    @Override
    public void refresh() throws ShepMBeanException { // TODO move to util class
	try {
	    this.conf = (ArchiverConf) JAXBUtils.refresh(ArchiverConf.class,
		    this.xmlFilePath);
	} catch (FileNotFoundException fnfe) {
	    this.conf = new ArchiverConf();
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepMBeanException(e);
	}
    }
}
