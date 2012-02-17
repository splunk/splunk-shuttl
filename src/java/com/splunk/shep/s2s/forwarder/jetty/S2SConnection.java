package com.splunk.shep.s2s.forwarder.jetty;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.eclipse.jetty.io.ByteArrayBuffer;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;

import com.splunk.shep.s2s.DataSink;
import com.splunk.shep.s2s.InvalidSignatureException;
import com.splunk.shep.s2s.S2SChannel;
import com.splunk.shep.s2s.S2SProtocolHandler;
import com.splunk.shep.s2s.S2SProtocolHandlerFactory;

public class S2SConnection implements Connection {

    Connector connector;
    EndPoint endpoint;
    Server server;
    S2SChannel s2schannel;

    ByteArrayBuffer buffer = new ByteArrayBuffer(new byte[40960]);

    private Logger logger = Logger.getLogger(getClass());

    public S2SConnection(Connector connector, EndPoint endpoint, Server server) {
	logger.trace("S2SConnection");
	this.connector = connector;
	this.endpoint = endpoint;
	this.server = server;
	try {
	    S2SConnector myconnector = (S2SConnector) this.connector;
	    String sinkclass = myconnector.getDataSink();
	    DataSink sink = (DataSink) Class.forName(sinkclass).newInstance();
	    sink.setName(myconnector.getName());
	    S2SProtocolHandler handler = S2SProtocolHandlerFactory
		    .createHandler(S2SProtocolHandlerFactory.VERSION_43);
	    handler.setSink(sink);
	    this.s2schannel = new S2SChannel(handler);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    @Override
    public void closed() {
	// TODO Auto-generated method stub
	logger.trace("closed");

    }

    @Override
    public long getTimeStamp() {
	// TODO Auto-generated method stub
	logger.trace("getTimeStamp");
	return 0;
    }

    @Override
    public Connection handle() throws IOException {
	// TODO Auto-generated method stub
	logger.trace("handle");
	try {
	    int read = this.endpoint.fill(buffer);
	    logger.debug("read # of bytes :" + read);
	    if (read != 0) {
		byte buf[] = this.buffer.asArray();
		this.s2schannel.dataAvailable(buf, 0, buf.length);
	    }
	} catch (java.net.SocketException se) {
	    this.endpoint.close();
	} catch (InvalidSignatureException ise) {
	    this.endpoint.close();
	} catch (Exception e) {
	    this.endpoint.close();
	    e.printStackTrace();
	}
	buffer.clear();
	return this;
    }

    @Override
    public void idleExpired() {
	// TODO Auto-generated method stub
	logger.trace("idleExpired");

    }

    @Override
    public boolean isIdle() {
	// TODO Auto-generated method stub
	logger.trace("isIdle");
	return false;
    }

    @Override
    public boolean isSuspended() {
	// TODO Auto-generated method stub
	logger.trace("isSuspended");
	return false;
    }

}
