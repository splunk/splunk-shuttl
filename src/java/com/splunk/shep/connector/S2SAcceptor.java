// S2SAcceptor.java
//
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

package com.splunk.shep.connector;

import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

public class S2SAcceptor implements Acceptor {
    private AcceptorConfig config;
    private S2SDataHandlerFactory factory;
    private Logger logger = Logger.getLogger(getClass());

    public S2SAcceptor(AcceptorConfig config, S2SDataHandlerFactory factory) {
	this.config = config;
	this.factory = factory;
    }

    @Override
    public String getBindIP() {
	return config.getBindIP();
    }

    @Override
    public int getPort() {
	return config.getPort();
    }

    @Override
    public Channel handle(SocketChannel socket) throws Exception {
	logger.debug("New Connection on port=" + config.getPort() + " from "
		+ socket.socket().getRemoteSocketAddress());
	return new S2SChannel(config, factory.createHandler());
    }
}

class S2SChannel implements Channel {
    private AcceptorConfig config;
    private S2SStateMachine stateMachine;
    private Logger logger = Logger.getLogger(getClass());

    public S2SChannel(AcceptorConfig config, S2SDataHandler handler) {
	this.config = config;
	stateMachine = new S2SStateMachine(handler);
    }

    @Override
    public void dataAvailable(byte[] buf, int offset, int len)
	    throws AbortConnectionException {
	logger.debug("Received bytes=" + buf.length + " on port="
		+ config.getPort());
	try {
	    stateMachine.consume(buf, offset, len);
	} catch (InvalidSignatureException aex) {
	    throw new AbortConnectionException(aex.getMessage());
	} catch (InvalidS2SDataException invS2SEx) {
	    logger.error(invS2SEx.getMessage());
	    throw new AbortConnectionException(invS2SEx.getMessage());
	} catch (Exception ex) {
	    logger.error(ex);
	    throw new AbortConnectionException(ex.getMessage());
	}
    }

    @Override
    public void connectionClosed(SocketChannel socket) {
	logger.debug("Connection to port=" + config.getPort() + " from "
		+ socket.socket().getRemoteSocketAddress().toString()
		+ " closed.");
    }
}
