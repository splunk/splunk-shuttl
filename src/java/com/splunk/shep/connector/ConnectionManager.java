// ConnectionManager.java
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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

public class ConnectionManager {
    private List<ConnectionManagerImpl> impls = new ArrayList<ConnectionManagerImpl>();
    private Logger logger = Logger.getLogger(getClass());

    public ConnectionManager() {
    }

    public void listen(List<Acceptor> acceptors) throws IOException {
	for (Acceptor acceptor : acceptors) {
	    listenOne(acceptor);
	}
    }

    private void listenOne(Acceptor acceptor) throws IOException {
	impls.add(new ConnectionManagerImpl(acceptor));
    }

    public void run() throws IOException {
	for (ConnectionManagerImpl impl : impls) {
	    impl.start();
	}
    }

    public void shutdown() {
	for (ConnectionManagerImpl impl : impls) {
	    impl.shutdown();
	}
	for (ConnectionManagerImpl impl : impls) {
	    try {
		impl.join();
	    } catch (InterruptedException e) {
		logger.error(e);
	    }
	}
    }
}

/**
 * Each Acceptor port is handled in a Thread. Each channel's data handler can
 * potentially block specially when it is trying to send data out. To make sure
 * that each Acceptor port is independent, i.e. it does not get affected by
 * channel in other acceptor port being blocked, we need to have each
 * Selector(ConnectionManager) be run on its own thread.
 */
class ConnectionManagerImpl extends Thread {
    private Acceptor acceptor;
    private ServerSocketChannel serverChannel;
    private Selector selector;
    private boolean stopManager = false;
    ByteBuffer buf = ByteBuffer.allocate(4096);
    private Logger logger = Logger.getLogger(getClass());

    public ConnectionManagerImpl(Acceptor acceptor) throws IOException {
	this.acceptor = acceptor;

	// Create a selector
	selector = Selector.open();

	// Bind to the local host
	InetSocketAddress addr = new InetSocketAddress(
		InetAddress.getByName(acceptor.getBindIP()), acceptor.getPort());

	serverChannel = ServerSocketChannel.open();
	serverChannel.socket().bind(addr);
	logger.info("Successfully bound port=" + acceptor.getPort());

	// Set non-blocking call
	serverChannel.configureBlocking(false);
	serverChannel.register(selector, SelectionKey.OP_ACCEPT, acceptor);
    }

    public void run() {
	while (!shouldShutdown()) {
	    try {
	    	select();
	    } catch (IOException e) {
	    	logger.error("Exception in running connection manager: " + e.toString() + "\nStacktrace:\n" + e.getStackTrace().toString());
	    }
	}

	logger.warn("Shutting down acceptor port=" + acceptor.getPort());
	try {
	    serverChannel.close();
	} catch (IOException e) {
	    logger.error(e);
	}
	serverChannel = null;
    }

    public void select() throws IOException {
	logger.debug("Waiting for connection on port=" + acceptor.getPort());
	while (selector.select(1000) > 0) {
	    Iterator<SelectionKey> it = selector.selectedKeys().iterator();
	    while (it.hasNext()) {
		SelectionKey key = it.next();
		it.remove();

		if (key.isAcceptable()) {
		    ServerSocketChannel serverSocket = (ServerSocketChannel) key
			    .channel();

		    SocketChannel socket = serverSocket.accept();
		    socket.configureBlocking(false);

		    Acceptor acceptor = (Acceptor) key.attachment();
		    Channel channel;
		    try {
			channel = acceptor.handle(socket);
			socket.register(selector, SelectionKey.OP_READ, channel);
			logger.debug("New connection on port="
				+ acceptor.getPort() + " from ip="
				+ socket.socket().getRemoteSocketAddress());
		    } catch (Exception e) {
			socket.close();
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			logger.error("Failed to handle connection from client on port="
				+ acceptor.getPort() + " " + sw.toString());
		    }
		}

		if (key.isReadable()) {
		    SocketChannel socket = (SocketChannel) key.channel();
		    Channel channel = (Channel) key.attachment();
		    int bytesRead = socket.read(buf);
		    if (bytesRead == -1) {
			logger.debug("Connection from ip="
				+ socket.socket().getRemoteSocketAddress()
				+ " closed by sender.");
			channel.connectionClosed(socket);
			socket.close();
		    } else {
			try {
			    channel.dataAvailable(buf.array(), 0, bytesRead);
			} catch (AbortConnectionException acex) {
			    // Channel handler does not want to continue with
			    // the connection. close it
			    logger.warn("Connection closed by application. "
				    + acex.getMessage());
			    socket.close();
			}
			buf.clear();
		    }
		}
	    } // while (it.hasNext()
	} // while (selector.select
    }

    private boolean shouldShutdown() {
	synchronized (this) {
	    return stopManager;
	}
    }

    public void shutdown() {
	synchronized (this) {
	    stopManager = true;
	    selector.wakeup();
	}
    }
}