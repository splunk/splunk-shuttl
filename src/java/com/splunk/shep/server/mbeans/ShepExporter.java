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
import java.util.List;

import org.apache.log4j.Logger;

import com.splunk.shep.server.mbeans.util.JAXBUtils;
import com.splunk.shep.server.model.ExporterConf;
import com.splunk.shep.server.model.ExporterConf.Channel;
import com.splunk.shep.server.services.SplunkExporterService;

/**
 * @author kpakkirisamy
 *
 */
public class ShepExporter implements ShepExporterMBean {
    // error messages
    private static final String M_BEAN_NOT_INITIALIZED_WITH_XML_BEAN = "MBean not initialized with xml bean";
    private static final String SHEP_EXPORTER_INIT_FAILURE = "ShepExporter init failure";
    // end error messages
    private static String EXPORTERCONF_XML = "etc/apps/shep/conf/exporter.xml";
    private Logger logger = Logger.getLogger(getClass());
    private String xmlFilePath;
    private ExporterConf conf;
    private SplunkExporterService service;

    public ShepExporter() throws ShepMBeanException {
	try {
	    this.xmlFilePath = System.getProperty("splunk.home")
		    + EXPORTERCONF_XML;
	    refresh();
	} catch (Exception e) {
	    logger.error(SHEP_EXPORTER_INIT_FAILURE, e);
	    throw new ShepMBeanException(e);
	}
    }

    // signature used by tests
    public ShepExporter(String confFile) throws ShepMBeanException {
	try {
	    this.xmlFilePath = confFile;
	    refresh();
	} catch (Exception e) {
	    logger.error(SHEP_EXPORTER_INIT_FAILURE, e);
	    throw new ShepMBeanException(e);
	}
    }

    public void setSplunkExporterService(SplunkExporterService service) {
	this.service = service;
    }

    /**
     * @see com.splunk.shep.server.model.ExporterConfiguration#getOutputPath()
     */
    @Override
    public String getOutputPath() {
	return this.conf.getOutputPath();
    }

    /**
     * @see com.splunk.shep.server.model.ExporterConfiguration#setOutputPath(java.lang.String)
     */
    @Override
    public void setOutputPath(String outputPath) {
	this.conf.setOutputPath(outputPath);
    }

    /**
     * @see com.splunk.shep.server.model.ExporterConfiguration#getTempPath()
     */
    @Override
    public String getTempPath() {
	return this.conf.getTempPath();
    }

    /**
     * @see com.splunk.shep.server.model.ExporterConfiguration#setTempPath(java.lang.String)
     */
    @Override
    public void setTempPath(String tempPath) {
	this.conf.setTempPath(tempPath);
    }

    /**
     * @see com.splunk.shep.server.model.ExporterConfiguration#getChannels()
     */
    @Override
    public List<Channel> getChannels() {
	return this.conf.getChannels();
    }

    /**
     * @see com.splunk.shep.server.model.ExporterConfiguration#setChannels(java.util.List)
     */
    @Override
    public void setChannels(List<Channel> channels) {
	this.conf.setChannels(channels);
    }

    /**
     * @see com.splunk.shep.server.mbeans.ShepExporterMBean#startExporterService()
     */
    @Override
    public void startExporterService() throws ShepMBeanException {
	try {
	    logger.debug("starting exporter service");
	    this.service.setExportConfiguration(this.conf);
	    this.service.start();
	} catch (Exception e) {
	    logger.error(e);
	    e.printStackTrace();
	    throw new ShepMBeanException(e);
	}
    }

    /**
     * @see com.splunk.shep.server.mbeans.ShepExporterMBean#stopExporterService()
     */
    @Override
    public void stopExporterService() throws ShepMBeanException {
	try {
	    this.service.stop();
	} catch (Exception e) {
	    logger.error(e);
	    e.printStackTrace();
	    throw new ShepMBeanException(e);
	}
    }

    @Override
    public String getStatus() {
	return this.service.getStatus();
    }

    /**
     * @see com.splunk.shep.server.mbeans.ShepExporterMBean#save()
     */
    @Override
    public void save() throws ShepMBeanException {
	try {
	    JAXBUtils.save(ExporterConf.class, this.conf, this.xmlFilePath);
	} catch (Exception e) {
	    logger.error(e);
	    e.printStackTrace();
	    throw new ShepMBeanException(e);
	}
    }

    /**
     * @see com.splunk.shep.server.mbeans.ShepExporterMBean#refresh()
     */
    @Override
    public void refresh() throws ShepMBeanException {
	try {
	    this.conf = (ExporterConf) JAXBUtils.refresh(ExporterConf.class,
		    this.xmlFilePath);
	} catch (FileNotFoundException fnfe) {
	    this.conf = new ExporterConf();
	} catch (Exception e) {
	    logger.error(e);
	    e.printStackTrace();
	    throw new ShepMBeanException(e);
	}
    }
}
