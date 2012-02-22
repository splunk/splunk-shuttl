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

/**
 * 
 * @author kpakkirisamy
 * 
 */
public class S2SConnection implements Connection {

    Connector connector;
    EndPoint endpoint;
    Server server;
    S2SChannel s2schannel;

    ByteArrayBuffer buffer = new ByteArrayBuffer(new byte[4096]);

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
	    logger.error("Exception in init ", e);
	    throw new RuntimeException("Exception in S2SConnection init");
	}
    }

    @Override
    public long getTimeStamp() {
	return 0;
    }

    @Override
    public void onIdleExpired(long l) {
    }

    @Override
    public void onClose() {
    }

    @Override
    public Connection handle() throws IOException {
	try {
	    int read = this.endpoint.fill(buffer);
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
	    logger.error("Exception in handle", e);
	}
	buffer.clear();
	return this;
    }

    @Override
    public boolean isIdle() {
	return false;
    }

    @Override
    public boolean isSuspended() {
	return false;
    }

}
