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
 *
 */
public class ShepArchiver implements ShepArchiverMBean {
    // error messages
    private static final String SHEP_ARCHIVER_INIT_FAILURE = "ShepArchiver init failure";
    private ArchiverConf conf;
    private static String ARCHIVERCONF_XML = "etc/apps/shep/conf/archiver.xml";
    private Logger logger = Logger.getLogger(getClass());
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
    public String getArchiverRoot() {
	return this.conf.getArchiverRoot();
    }

    @Override
    public void setArchiverRoot(String archiverRoot) {
	this.conf.setArchiverRoot(archiverRoot);
    }

    @Override
    public String getClusterName() {
	return this.conf.getClusterName();
    }

    @Override
    public void setClusterName(String clusterName) {
	this.conf.setClusterName(clusterName);
    }

    @Override
    public void setIndexNames(List<String> indexNames) {
	this.conf.setIndexNames(indexNames);
    }

    @Override
    public List<String> getIndexNames() {
	return this.conf.getIndexNames();
    }

    @Override
    public void addIndex(String name) {
	if (this.conf.getIndexNames() == null) {
	    this.conf.setIndexNames(new ArrayList<String>());
	}
	this.conf.getIndexNames().add(name);
    }

    @Override
    public void deleteIndex(String name) {
	this.conf.getIndexNames().remove(name);
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
