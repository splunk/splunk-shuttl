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

import java.io.FileReader;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;

import com.splunk.shep.server.model.ForwarderConf;

/**
 * 
 * @author kpakkirisamy
 * 
 */
public class ShepForwarder implements ShepForwarderMBean {
    private Logger logger = Logger.getLogger(getClass());
    private String FORWARDERCONF_XML = "etc/apps/shep/default/ForwarderConf.xml";
    private ArrayList<ForwarderConf.HDFSSink> hdfsSinkList;

    public ShepForwarder() throws ShepMBeanException {
	try {
	    String splunkhome = System.getProperty("splunk.home");
	    JAXBContext context = JAXBContext.newInstance(ForwarderConf.class);
	    Unmarshaller um = context.createUnmarshaller();
	    ForwarderConf conf = (ForwarderConf) um.unmarshal(new FileReader(
		    splunkhome + FORWARDERCONF_XML));
	    this.hdfsSinkList = conf.getHdfssinklist();
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepMBeanException(e);
	}
    }

    @Override
    public String getHDFSSinkPrefix(String name) throws ShepMBeanException {
	for (ForwarderConf.HDFSSink sink : this.hdfsSinkList) {
	    if (sink.getName().equals(name)) {
		return sink.getFileprefix();
	    }
	}
	throw new ShepMBeanException("Unable to find sink configuration: "
		+ name);
    }

    @Override
    public int getHDFSSinkMaxEventSize(String name) throws ShepMBeanException {
	for (ForwarderConf.HDFSSink sink : this.hdfsSinkList) {
	    if (sink.getName().equals(name)) {
		return sink.getMaxEventSizeKB();
	    }
	}
	throw new ShepMBeanException("Unable to find sink configuration: "
		+ name);
    }

    @Override
    public boolean getHDFSSinkUseAppending(String name)
	    throws ShepMBeanException {
	for (ForwarderConf.HDFSSink sink : this.hdfsSinkList) {
	    if (sink.getName().equals(name)) {
		return sink.isUseAppending();
	    }
	}
	throw new ShepMBeanException("Unable to find sink configuration: "
		+ name);
    }

}
