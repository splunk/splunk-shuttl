// EventExtractor.java
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

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.spi.*;
import java.nio.charset.*;
import java.net.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.text.DateFormat;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class EventExtractor 
{
	 
	private int listenPort = 9997;
	
	//private EventEmitter emitter = null;
	private int targetPort = 8888;
	private String targetIP = null;
	
	private Selector selector = null;
	private ServerSocketChannel serverChannel = null;
	private int keysAdded = 0;
	
	private boolean debugging = false;
	
	private LinkedList<EventChannel> channelList = null;
	
	private static Logger logger = Logger.getLogger(EventExtractor.class);
	
	
	public EventExtractor(int port, int tarPort, String tarIP)
	{
		listenPort = port;
		//emitter = sender;
		targetPort = tarPort;
		targetIP = tarIP;
	}
	
	public EventExtractor(int port)
	{
		listenPort = port;
		targetPort = -1;
		targetIP = null;
	}
	
	public void debug()
	{
		//logger.setLevel(Level.DEBUG);
		logger.debug("set debug mode");
		debugging = true;
		//if (emitter != null)
		//	emitter.debug();
	}
	
	
	public void run() throws Exception
	{
		logger.info("started at port " + listenPort);
		
		channelList = new LinkedList<EventChannel>();
		
		selector = Selector.open();
		serverChannel = ServerSocketChannel.open();
		serverChannel.configureBlocking(false);
		InetSocketAddress isa = new InetSocketAddress( listenPort );
		serverChannel.socket().bind(isa);
		SelectionKey acceptKey = serverChannel.register( selector, SelectionKey.OP_ACCEPT );
		
		int listenLoop = 1;
		
		while ( true ) 
		{
			keysAdded = acceptKey.selector().select();
			if ( keysAdded > 0 )
			{
				logger.info( "Selector returned " + keysAdded + " ready for IO operations" );
			
				Set readyKeys = this.selector.selectedKeys();
				Iterator i = readyKeys.iterator();
			
				if (i.hasNext()) 
				{
					SelectionKey key = (SelectionKey)i.next();
					i.remove();
				
					if ( key.isAcceptable() ) 
					{
						ServerSocketChannel nextReady = (ServerSocketChannel)key.channel();
					
						logger.debug( "Processing selection key read="
							  + key.isReadable() + " write=" + key.isWritable() +
							  " accept=" + key.isAcceptable() );
					
						SocketChannel channel = nextReady.accept();
						//channel.configureBlocking( false );
						//SelectionKey readKey = channel.register( this.selector, SelectionKey.OP_READ|SelectionKey.OP_WRITE  );
						//readKey.attach( new ChannelCallback( channel ) );
					
						int id = channelList.size();
						EventChannel connection = new EventChannel(channel, targetIP, targetPort, id, debugging);
						connection.start();
						channelList.add(connection);
					}
				}
			}
			else
			{
				if (debugging)
					System.err.println("DEBUG: select " + listenLoop);
				
				if ((listenLoop % 30) == 0)
					cleanChannel();
				
				Thread.sleep(1);
			}
			
			//Thread.sleep(1);
			listenLoop++;
			logger.info("*** listen to port ***" + listenPort);
        }
		
	}
	
	public void cleanChannel()
	{
		try {
			
		logger.debug("channels: " + channelList.size());
		
		int i = 0;
		while (true) {
			int idxToRemove = -1;
			for (i = 0; i < channelList.size(); i++)
			{
				if (channelList.get(i).done())
				{
					idxToRemove = i;
					break;
				}
			}
			
			if ((idxToRemove >= 0) && (idxToRemove < channelList.size()) )
			{
				channelList.remove(idxToRemove);
				logger.info("removed channel " + idxToRemove);
			}
			else {
				break;
			}
		}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed cleaning channels");
		}
	}
	
	public void closeAll()
	{
		try {
			//serverSock.close();
			serverChannel.close();
			selector.close();
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) throws Exception
	{
		int recvPort = 9997;
		EventExtractor extractor = null;
		
		if (args.length > 0)
		{
			recvPort = Integer.parseInt(args[0]);
			if (recvPort <= 0)
				recvPort = 9997;
			
			if (args.length > 1)
				PropertyConfigurator.configure(args[1]);
		}
		else {
			System.err.println("Usage: EventExtractor <recv-port> [<properties-file>]");
			return;
		}
		
		try {
			extractor = new EventExtractor(recvPort);
			extractor.debug();
			extractor.run();
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
		finally {
			extractor.closeAll();
		}
	}
}


class EventChannel extends Thread
{
	private static final String TCPSIGNATURE_COOKED_SIG = new String("--splunk-cooked-mode--");
	private static final String TCPSIGNATURE_COOKED_SIG_V2 = new String("--splunk-cooked-mode-v2--");
	private static final String TCPSIGNATURE_COOKED_SIG_V3 = new String("--splunk-cooked-mode-v3--");
	
	private static final int SigSize = 128;
	private static final int SvrNameSize = 256;
	private static final int PortSize = 16;
	
	private int channelID = -1;
	private SocketChannel clientChannel = null;
	private InputStream sockIn = null;
	
	int targetPort = 8888;
	String targetIP = null;
	//private EventEmitter emitter = null;
	private HdfsIO emitter = null;
	
	private boolean debugging = false;
	
	// states
	private static final int Born = 0;
	private static final int Running = 1;
	private static final int Stopping = -1;
	private static final int Finished = -2;
	
	private int state = 0;
	
	private static Logger logger = Logger.getLogger(EventChannel.class);
	
	
	public EventChannel(SocketChannel channel, String tarIP, int tarPort, int id, boolean dbgFlag)
	{
		clientChannel = channel;
		channelID = id;
		targetIP = tarIP;
		targetPort = tarPort;
		//emitter = sender;
		state = Born;
		if (dbgFlag) debug();
	}
	
	public void debug()
	{
		//logger.setLevel(Level.DEBUG);
		//logger.debug("set debug mode");
		//debugging = true;
		//if (emitter != null)
		//	emitter.debug();
	}
	
	
	public void run()
	{
		try 
		{
			state = Running;
			
			if (targetPort > 0)
			{
				//emitter = new EventEmitter(targetPort, targetIP);
				//emitter.start();
				emitter = new HdfsIO();
				emitter.start("/xli/spldata");
			}
			sockIn = clientChannel.socket().getInputStream();
			//OutputStream sockOut = sock.getOutputStream();
			
		
			while (true) 
			{
				Thread.sleep(50);
				
				// Check signature first.
				// signature portion: <sig 128 bytes> + <serverName 256 bytes> + <mgmt port 16 bytes>
				int sigHeadSize = SigSize + SvrNameSize + PortSize;
				int readLen = 0;
				
				byte[] sigBuf = new byte[SigSize];
				
				try 
				{
					do {
						int numBytes = sockIn.read(sigBuf, readLen, (SigSize - readLen));
						if (numBytes < 0)
							throw (new IOException("WARN: lost client connection"));
						
						readLen += numBytes;
						logger.debug("received " + readLen + " bytes");
					} while (readLen < SigSize);
					
					String sig = new String(sigBuf);
					if (debugging)
						System.err.println("DEBUG: received Signature bytes: " + sig + " " + sig.length());
					sig = sig.replace('\0', ' ');
					sig = sig.trim();
					if (debugging)
						System.err.println("DEBUG: received s2s signature: " + sig + " " + sig.length());
					
					if( (sig.indexOf(TCPSIGNATURE_COOKED_SIG_V3)) >= 0 )
					{
						logger.info("channel " + channelID + " signature " + TCPSIGNATURE_COOKED_SIG_V3 + " not supported. Quit");
						break;
					}
					logger.info("channel " + channelID + " accept signature " + sig);
					
					// Receiving server and port information.
					byte[] svrNameBuf = new byte[SvrNameSize];
					readLen = 0;
					do {
						int numBytes = sockIn.read(svrNameBuf, readLen, (SvrNameSize - readLen));
						if (numBytes < 0)
							throw (new IOException("channel " + channelID + " lost client connection"));
						
						readLen += numBytes;
						logger.debug("channel " + channelID + " received " + readLen + " bytes");
					} while (readLen < SvrNameSize);
					
					String svrName = new String(svrNameBuf);
					logger.debug("channel " + channelID + " Server Name: " + svrName);
					
					byte[] portBuf = new byte[PortSize];
					readLen = 0;
					do {
						int numBytes = sockIn.read(portBuf, readLen, (PortSize - readLen));
						if (numBytes < 0)
							throw (new IOException("channel " + channelID + " lost client connection"));
						
						readLen += numBytes;
						logger.debug("channel " + channelID + " received " + readLen + " bytes");
					} while (readLen < PortSize);
					String connPort = new String(portBuf);
					logger.debug("channel " + channelID + " port: " + connPort);
					
					// Receiving events.
					EventParser receiver = null;
					if (emitter != null) {
						receiver = new EventParser(sockIn, emitter, channelID);
					}
					else {
						receiver = new EventParser(sockIn, channelID);
					}
					
					if (debugging)
						receiver.debug();
					
					receiver.readData();
					//recver.displayData();
				} 
				catch (IOException ex) {
					ex.printStackTrace();
					logger.warn("channel " + channelID + " lost connection to client");
					break;
				}
				catch (Exception ex) {
					ex.printStackTrace();
					logger.warn("channel " + channelID + " caught exception, retry connection");
					break;
				}
			} 
			
			//closeClient();
		}
		catch (Exception ex) {
			ex.printStackTrace();
			logger.error("channel " + channelID + " caught exception, channel exit");
		}
		finally {
			closeClient();
			state = Finished;
			logger.info("channel " + channelID + " socket channel end");
		}
	}
	
	private void closeSockStream()
	{
		try {
			if (sockIn != null)
				sockIn.close();
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	private void closeClient()
	{
		try {
			closeSockStream();
			clientChannel.close();
			if (emitter != null)
				emitter.close();
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public boolean running()
	{
		return (state == Running) ? true : false;
	}
	
	public boolean done()
	{
		return (state == Finished) ? true : false;
	}
	
	public void quit()
	{
		state = Stopping;
	}
}
