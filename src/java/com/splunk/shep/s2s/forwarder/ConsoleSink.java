package com.splunk.shep.s2s.forwarder;

import org.apache.log4j.Logger;

import com.splunk.shep.s2s.DataSink;

public class ConsoleSink implements DataSink {

    private Logger logger = Logger.getLogger(getClass());

    public ConsoleSink() {
	logger.info("init");
    }

    @Override
    public void setName(String name) {

    }

    @Override
    public void start() throws Exception {
	// TODO Auto-generated method stub
	logger.info("start");
    }

    @Override
    public void start(String sinkPath) throws Exception {
	// TODO Auto-generated method stub
	logger.info("start");
    }

    @Override
    public void close() {
	// TODO Auto-generated method stub
	logger.info("close");
    }

    @Override
    public void send(byte[] rawBytes, String sourceType, String source,
	    String host, long time) throws Exception {
	// TODO Auto-generated method stub
	System.out.println("bytes " + new String(rawBytes, "UTF-8"));
	System.out.println("bytes " + sourceType + " " + source + " " + host
		+ " " + time);

    }

    @Override
    public void send(String data, String sourceType, String source,
	    String host, long time) throws Exception {
	// TODO Auto-generated method stub
	System.out.println("bytes str:" + data);
	System.out.println("bytes str" + sourceType + " " + source + " " + host
		+ " " + time);
    }

    @Override
    public void send(byte[] rawBytes) throws Exception {
	// TODO Auto-generated method stub
	System.out.println("byte[] " + new String(rawBytes, "UTF-8"));

    }

    @Override
    public void setMaxEventSize(long size) {
	// TODO Auto-generated method stub

    }

    @Override
    public void setFileRollingSize(long size) {
	// TODO Auto-generated method stub

    }

}
