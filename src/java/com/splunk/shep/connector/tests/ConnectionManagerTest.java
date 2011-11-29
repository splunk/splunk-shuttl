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

package com.splunk.shep.connector.tests;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.splunk.shep.connector.Acceptor;
import com.splunk.shep.connector.BridgeConfig;
import com.splunk.shep.connector.ConnectionManager;
import com.splunk.shep.connector.S2SAcceptor;

import org.apache.log4j.*;

public class ConnectionManagerTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Receiver r = new Receiver();
		r.start();
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		Sender s = new Sender();
		s.start();
		
		try {
			s.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		r.stopReceiver();
		try {
			r.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

class Sender extends Thread {
	private Socket sock1;
	private Socket sock2;
	private Logger logger = Logger.getLogger(getClass());
	private boolean connect1() throws InterruptedException {
		int attempt = 0;
		while(attempt++ < 5) {
			try {
				sock1 = new Socket("localhost", 9997);
				return true;
			} catch (UnknownHostException e) {
				Thread.sleep(100);
			} catch (IOException e) {
				Thread.sleep(100);
			}
		}
		System.err.println("Could not connect to port 9997");
		return false;
	}
	private boolean connect2() throws InterruptedException {
		int attempt = 0;
		while(attempt++ < 5) {
			try {
				sock2 = new Socket("localhost", 9998);
				return true;
			} catch (UnknownHostException e) {
				Thread.sleep(100);
			} catch (IOException e) {
				Thread.sleep(100);
			}
		}
		System.err.println("Could not connect to port 9998");
		return false;
	}
	
	private void doIt() throws InterruptedException, IOException {
		// send messages
		
		if (!connect1())
			return;
		if (!connect2())
			return;
		try {
			PrintWriter pw1 = new PrintWriter(sock1.getOutputStream());
			PrintWriter pw2 = new PrintWriter(sock2.getOutputStream());
			
			for (int i=0; i < 100; i++) {
				logger.info("Message for acceptor 1. Message id=" + i);
				pw1.println("Message for acceptor 1. Message id=" + i);
				
				logger.info("Message for acceptor 2. Message id=" + i);
				pw2.println("Message for acceptor 2. Message id=" + i);
			}
			pw1.close();
			sock1.close();
			sock1 = null;
			pw2.close();
			sock2.close();
			sock2 = null;
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			if (sock1 != null)
				sock1.close();
			if (sock2 != null)
				sock2.close();
		}
	}
	
	public void run() {
		try {
			doIt();
			Thread.sleep(2000);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.debug("Sender finished");
	}
}

class Receiver extends Thread {
	private ConnectionManager cm = new ConnectionManager();
	Logger logger = Logger.getLogger(getClass());
	public void run() {
		logger.debug("Starting Receivers");
		List<Acceptor> acceptors = new ArrayList<Acceptor>();
		Acceptor acc = new TestAcceptor("localhost", 9997);
		acceptors.add(acc);

		acc = new TestAcceptor("localhost", 9998);
		acceptors.add(acc);
		try {
			cm.listen(acceptors);
			cm.run();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.debug("Receivers started");
	}
	
	public void stopReceiver() {
		logger.debug("Shutting receivers");
		cm.shutdown();
		logger.debug("Receivers shutdown");
	}
}