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

package com.splunk.connector.tests;

import java.nio.channels.SocketChannel;
import org.apache.log4j.Logger;

import com.splunk.connector.AbortConnectionException;
import com.splunk.connector.Acceptor;
import com.splunk.connector.Channel;


public class TestAcceptor implements Acceptor {
	private String bindIP;
	private int port;
	public TestAcceptor(String bindIP, int port) {
		this.bindIP = bindIP;
		this.port = port;
	}
	@Override
	public String getBindIP() {
		return bindIP;
	}

	@Override
	public int getPort() {
		return port;
	}

	@Override
	public Channel handle(SocketChannel socket) {
		return new TestChannel(socket);
	}
}

class TestChannel implements Channel {
	private SocketChannel socket;
	private Logger logger = Logger.getLogger(getClass());
	public TestChannel(SocketChannel socket) {
		this.socket = socket;
	}
	
	@Override
	public void dataAvailable(byte[] buf, int offset, int len) throws AbortConnectionException {
		logger.info("Received " + buf.length + " bytes from " + socket.socket().getRemoteSocketAddress());
	}

	@Override
	public void connectionClosed(SocketChannel socket) {
		logger.info("Connection from " + socket.socket().getRemoteSocketAddress() + " closed");
	}
	
}