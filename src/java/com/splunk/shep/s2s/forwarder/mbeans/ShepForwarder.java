package com.splunk.shep.s2s.forwarder.mbeans;

import java.io.FileReader;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;

import com.splunk.shep.s2s.forwarder.model.ForwarderConf;

public class ShepForwarder implements ShepForwarderMBean {
    private Logger logger = Logger.getLogger(getClass());
    private String FORWARDERCONF_XML = "etc/apps/shep/default/ForwarderConf.xml";
    private ArrayList<ForwarderConf.HDFSSink> hdfssinklist;

    public ShepForwarder() throws Exception {
	logger.info("shepforwarder");
	try {
	    String splunkhome = System.getProperty("splunk.home");
    	JAXBContext context = JAXBContext.newInstance(ForwarderConf.class);
    	Unmarshaller um = context.createUnmarshaller();
    	ForwarderConf conf = (ForwarderConf) um.unmarshal(new FileReader(
    			splunkhome+FORWARDERCONF_XML));
    	this.hdfssinklist = conf.getHdfssinklist();
	} catch (Exception e) {
	    logger.info("shepforwarder exception");
	    e.printStackTrace();
	}
    }

    @Override
    public String getHDFSSinkPrefix(String name) {
	logger.info("getHDFSSinkPrefix") ;
	for(int i=0;i<this.hdfssinklist.size();i++) {
	    logger.info("iter getHDFSSinkPrefix " + i) ;
	    ForwarderConf.HDFSSink sink = this.hdfssinklist.get(i);
	    if (sink.getName().equals(name)) {
		return sink.getFileprefix();
	    }
	}
	return null;
    }

    @Override
    public int getHDFSSinkMaxEventSize(String name) {
	for(int i=0;i<this.hdfssinklist.size();i++) {
	    ForwarderConf.HDFSSink sink = this.hdfssinklist.get(i);
	    if (sink.getName().equals(name)) {
		return sink.getMaxEventSizeKB();
	    }
	}
	return 0;
    }

    @Override
    public boolean getHDFSSinkUseAppending(String name) {
	for(int i=0;i<this.hdfssinklist.size();i++) {
	    ForwarderConf.HDFSSink sink = this.hdfssinklist.get(i);
	    if (sink.getName().equals(name)) {
		return sink.isUseAppending();
	    }
	}
	return false;
    }

}
