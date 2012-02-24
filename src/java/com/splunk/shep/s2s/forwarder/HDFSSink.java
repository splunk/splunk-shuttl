// HdfsIO.java
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

package com.splunk.shep.s2s.forwarder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.URI;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

import com.splunk.shep.s2s.DataSink;

public class HDFSSink implements DataSink {
    public static final long HadoopFileSize = 63000000;
    private String ip = new String("localhost");
    private String port = new String("54310");
    private String path = new String("/spl");

    private Configuration conf = new Configuration();
    private FileSystem fileSystem = null;
    private Path destination = null;
    private FSDataOutputStream ofstream = null;
    private FSDataInputStream ifstream = null;

    private boolean useAppend = false;
    private boolean reopenedForWriting = false;
    private long totalBytesWritten = 0; // total bytes with current connection.
    private long fileRollingSize = HadoopFileSize;
    private long fileRollingDuration = 30000; // roll to new file every 30 sec.
    private long timeOpened = 0; // milliseconds
    private long maxEventSize = 32000; // not supported for now.
    private String currentFilePath = new String("");
    private String myname;

    private Logger logger = Logger.getLogger(getClass());

    public HDFSSink() {
    }
    
    // only when my name gets set I can init myself
    @Override
    public void setName(String name) {
	this.myname = name;
	try {
	    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

	    // Construct the ObjectName for the MBean we will register
	    ObjectName servername = new ObjectName(
		    "com.splunk.shep.mbeans:type=Server");
	    Object host = mbs.getAttribute(servername, "DefHadoopClusterHost");
	    this.ip = host.toString();
	    Object port = mbs.getAttribute(servername, "DefHadoopClusterPort");
	    this.port = port.toString();
	    ObjectName forwardername = new ObjectName(
		    "com.splunk.shep.mbeans:type=Forwarder");
	    Object params[] = new Object[1];
	    params[0] = new String(name);
	    String signature[] = { "java.lang.String" };
	    Object prefix = mbs.invoke(forwardername, "getHDFSSinkPrefix", params,
		    signature);
	    this.path = prefix.toString();
	    Object appending = mbs.invoke(forwardername,
		    "getHDFSSinkUseAppending", params,
		    signature);
	    this.useAppend = Boolean.getBoolean(appending.toString());
	    init();
	} catch (Exception e) {
	    // should not happen - so runtime exception
	    logger.error("HDFSSink Init fail in setName" , e);
	    throw new RuntimeException("HDFSSink Init fail in setName");
	}
    }

    public HDFSSink(String targetIP, String targetPort, String filePath,
	    boolean useAppending)
	    throws Exception {
	port = targetPort;
	ip = targetIP;
	path = filePath;
	useAppend = useAppending;
	init();
    }

    public HDFSSink(String targetIP, String targetPort) throws Exception {
	port = targetPort;
	ip = targetIP;
	path = new String("");
	init();
    }

    private void init() throws Exception {
	try {
	    conf = new Configuration();
	    String uri = new String("hdfs://") + ip + ':' + port;
	    fileSystem = FileSystem.get(URI.create(uri), conf);
	} catch (Exception e) {
	    logger.error("Exception in setting HDFS configuration: "
		    + e.toString());
	    throw (e);
	}
    }

    public void set(String filePath) {
	if (filePath != null) {
	    path = filePath;
	} else if (ofstream != null)
	    return; // already set.

	close();

	if (path.charAt(0) != '/')
	    path = "/" + path; // must be absolute path.

	try {
	    // Loop a few times to make sure to get a new file name.
	    for (int i = 0; i < 10; i++) {
		currentFilePath = path + System.currentTimeMillis();
		String tarURL = new String("hdfs://") + ip + ':' + port
		    + currentFilePath;
		destination = new Path(tarURL);

		if (!fileSystem.exists(destination)) {
		    break;
		    // ofstream = fileSystem.append(destination);
		    // logger.info("Append to: " + tarURL);
		}
	    }

	    ofstream = fileSystem.create(destination);
	    totalBytesWritten = 0;
	    timeOpened = System.currentTimeMillis();
	    reopenedForWriting = false;
	    logger.info("Write to: " + currentFilePath);

	} catch (Exception e) {
	    logger.error("Exception in setting Hadoop connection: "
		    + e.toString() + "\nStacktrace:\n"
		    + e.getStackTrace().toString());
	}
    }

    public boolean setFilePath(String filePath) {
	close(); // close any existing connection.
	if (filePath != null) {
	    path = filePath;
	} else if (ofstream != null)
	    return false; // already set.

	if (path.charAt(0) != '/')
	    path = "/" + path; // must be absolute path.

	try {
	    currentFilePath = path + System.currentTimeMillis();
	    String tarURL = new String("hdfs://") + ip + ':' + port
		    + currentFilePath;
	    destination = new Path(tarURL);

	    if (!fileSystem.exists(destination)) {
		return false;
	    }
	} catch (Exception e) {
	    logger.error("Exception in setting Hadoop fiel path: "
		    + e.toString() + "\nStacktrace:\n"
		    + e.getStackTrace().toString());
	}
	return true;
    }

    // Set connection to the current file.
    private boolean openCurrentFile(boolean reading) {
	close(); // close any existing connection.

	if (currentFilePath.length() == 0)
	    return false;

	try {
	    String tarURL = new String("hdfs://") + ip + ':' + port
		    + currentFilePath;
	    destination = new Path(tarURL);

	    if (!fileSystem.exists(destination)) {
		return false;
	    }

	    if (reading)
		return openRead();
	    else
		return openAppend();

	} catch (Exception e) {
	    logger.error("Exception in setting Hadoop file path: "
		    + e.toString() + "\nStackTrace:\n"
		    + e.getStackTrace().toString());
	}
	return false;
    }

    public boolean openRead() throws Exception {
	if (destination == null)
	    return false;
	
	if (!fileSystem.exists(destination))
		return false;
		
	ifstream = fileSystem.open(destination);
	return true;
    }

    public boolean openAppend() throws Exception {
	if (destination == null)
	    return false;

	if (!fileSystem.exists(destination))
	    return false;

	ofstream = fileSystem.append(destination);
	reopenedForWriting = true;
	timeOpened = System.currentTimeMillis();
	return true;
    }

    // Create current file.
    private boolean createCurrentFile() {
	close(); // close any existing connection.

	if (currentFilePath.length() == 0)
	    return false;

	try {
	    String tarURL = new String("hdfs://") + ip + ':' + port
		    + currentFilePath;
	    destination = new Path(tarURL);

	    if (fileSystem.exists(destination)) {
		return false;
	    }

	    ofstream = fileSystem.create(destination);
	    totalBytesWritten = 0;
	    timeOpened = System.currentTimeMillis();
	    reopenedForWriting = false;
	    logger.info("Write to: " + currentFilePath);

	} catch (Exception e) {
	    logger.error("Exception in setting Hadoop file path: "
		    + e.toString() + "\nStackTrace:\n"
		    + e.getStackTrace().toString());
	}
	return true;
    }

    public String getCurrentFileName() {
	return currentFilePath;
    }

    public void setFileRollingSize(long size) {
	fileRollingSize = size;
	logger.info("HDFS file rolling size: " + fileRollingSize);
    }

    public void setFileRollingDuration(long interval) {
	fileRollingDuration = interval;
	logger.info("HDFS file rolling duration: " + fileRollingDuration);
    }

    public void setMaxEventSize(long size) {
	maxEventSize = size;
	logger.info("Ignored max event size: " + maxEventSize);
    }

    public void setAppending(boolean appending) {
	useAppend = appending;
	if (useAppend)
	    logger.info("Appending enabled.");
	else
	    logger.info("Appending disabled.");
    }

    public String getPort() {
	return port;
    }

    public String getIp() {
	return ip;
    }

    public void start(String fileNameAndPath) throws Exception {
	logger.info("Start connection to hdfs at " + fileNameAndPath);
	set(fileNameAndPath);
    }

    public void start() throws Exception {
	start(path);
    }
    
    // open existing file - for reading only.
    public boolean openToRead(String fileName) throws Exception {
	close(); // close any existing connection.
	if (fileName != null) {
	    currentFilePath = fileName;
	} else
	    return false;

	if (currentFilePath.charAt(0) != '/')
	    currentFilePath = "/" + fileName; // must be absolute path.

	logger.info("Open connection to hdfs file " + currentFilePath);
	openCurrentFile(true);
	return true;
    }

    public boolean openToAppend(String fileName) throws Exception {
	close(); // close any existing connection.
	if (fileName != null) {
	    currentFilePath = fileName;
	} else
	    return false;

	if (currentFilePath.charAt(0) != '/')
	    currentFilePath = "/" + fileName; // must be absolute path.

	logger.info("Open connection to hdfs file " + currentFilePath);
	openCurrentFile(false);
	return true;
    }

    // open a new file - for writing.
    public boolean openToCreate(String fileName) throws Exception {
	close(); // close any existing connection.
	if (fileName != null) {
	    currentFilePath = fileName;
	} else
	    return false;

	if (currentFilePath.charAt(0) != '/')
	    currentFilePath = "/" + fileName; // must be absolute path.

	logger.info("Open connection to hdfs file " + currentFilePath);
	return createCurrentFile();
    }

    public void close() {
	try {
	    boolean closed = false;

	    if (ofstream != null) {
		ofstream.close();
		ofstream = null;
		closed = true;
	    }

	    if (ifstream != null) {
		ifstream.close();
		ifstream = null;
		closed = true;
	    }

	    if (closed) {
		logger.info("closed: " + path + " with size "
			+ totalBytesWritten);
		// totalBytesWritten = 0;
		timeOpened = 0;
	    }
	} catch (Exception e) {
	    logger.error("Exception in closing Hadoop connection: "
		    + e.toString() + "\nStacktrace:\n"
		    + e.getStackTrace().toString());
	}
    }

    private void checkFileSize() {
	if (totalBytesWritten < fileRollingSize) {
	    if (totalBytesWritten > 0) {
		logger.debug("Total bytes written so far: " + totalBytesWritten);
		checkOpenDuration();
	    }

	    return;
	}

	if (ofstream == null)
	    return;

	try {
	    start();
	} catch (Exception e) {
	    logger.error("Exception in reconnecting HDFS: " + e.toString());
	}
    }

    // Check time duration for closing/reopening.
    public void checkOpenDuration() {
	if (reopenedForWriting)
	    return; // reopened already.

	if (useAppend) {
	    try {
		close(); // close first
		Thread.sleep(1000);
		openCurrentFile(false); // reopen for writing.
	    } catch (Exception e) {
		logger.error("Exception in reconnecting HDFS: " + e.toString());
	    }
	    return;
	}

	if (timeOpened == 0)
	    return;

	if (ofstream == null)
	    return;

	if (totalBytesWritten <= 0)
	    return;

	if ((System.currentTimeMillis() - timeOpened) < fileRollingDuration) {
	    return;
	}

	try {
	    start();
	} catch (Exception e) {
	    logger.error("Exception in reconnecting HDFS: " + e.toString());
	}
    }

    public long getFileModTime() throws Exception {
	if (destination == null)
	    return -1;
	if (fileSystem.exists(destination)) {
	    FileStatus fileStatus = fileSystem.getFileStatus(destination);
	    return fileStatus.getModificationTime();
	}
	return -1;
    }

    // Implementation of DataSink interface method.
    public void send(byte[] rawBytes, String sourceType, String source,
	    String host, long time) throws Exception {
	String msg = new String(rawBytes);
	write(msg, sourceType, source, host, time);
	checkFileSize();
    }

    public void write(byte[] rawBytes, String sourceType, String source,
	    String host, long time) throws Exception {
	String message = HDFSEvent.build(rawBytes, sourceType, source, host,
		time);
	logger.debug("Sending " + message);

	if (ofstream == null) {
	    logger.warn("Cannot write: need to set connection.");
	    start();

	    if (ofstream == null) {
		logger.error("Failed writing data: connection not set.");
		return;
	    }
	}

	write(message);
    }

    // Implementation of DataSink interface method.
    public void send(String data, String sourceType, String source,
	    String host, long time) throws Exception {
	write(data, sourceType, source, host, time);
	checkFileSize();
    }

    public void write(String data, String sourceType, String source,
	    String host, long time) throws Exception {
	String message = HDFSEvent.build(data, sourceType, source, host, time);
	logger.debug("Sending " + message);

	if (ofstream == null) {
	    logger.warn("Cannot write: need to set connection.");
	    start();

	    if (ofstream == null) {
		logger.error("Failed writing data: connection not set.");
		return;
	    }
	}

	write(message);
    }

    private void write(String data) {
	try {
	    ofstream.writeUTF(data);
	    ofstream.flush();
	    logger.debug("Sent 1 event to Hadoop.");
	    totalBytesWritten += data.length();
	} catch (IOException e) {
	    logger.error("IOException during sending data: " + e.toString()
		    + "\nStacktrace:\n" + e.getStackTrace().toString());
	}
    }

    // Implementation of DataSink interface method.
    public void send(byte[] rawBytes) throws Exception {
	write(rawBytes);
	checkFileSize();
    }

    public void write(byte[] rawBytes) throws Exception {
	if (rawBytes == null)
	    return;

	if (ofstream == null) {
	    logger.warn("Cannot write: need to set connection.");
	    start();

	    if (ofstream == null) {
		logger.error("Failed writing data: connection not set.");
		return;
	    }
	}

	try {
	    ofstream.write(rawBytes, 0, rawBytes.length);
	    ofstream.flush();
	    logger.debug("Sent 1 event to Hadoop.");
	    totalBytesWritten += rawBytes.length;
	} catch (IOException e) {
	    logger.error("IOException in writing bytes: " + e.toString());
	    throw (e);
	}
    }

    public void setInputFile(String fileNamePath) throws Exception {
	if (fileNamePath == null)
	    throw (new Exception("Invalid file name to read."));

	try {
	    currentFilePath = fileNamePath;
	    if (currentFilePath.charAt(0) != '/')
		currentFilePath = "/" + fileNamePath;

	    String tarURL = new String("hdfs://") + ip + ':' + port
		    + currentFilePath;
	    destination = new Path(tarURL);
	    setInputFile();
	} catch (Exception e) {
	    logger.error("IOException in reading hdfs file " + currentFilePath
		    + ": "
		    + e.toString());
	    throw (e);
	}
    }

    // Read a line of current file.
    // Return null if end of data.
    private String readLine() {
	try {
	    if (ifstream == null)
		setInputFile();
	    LineReader reader = new LineReader(ifstream);
	    org.apache.hadoop.io.Text msg = new org.apache.hadoop.io.Text();
	    reader.readLine(msg);
	    if (msg.getLength() > 0)
		return msg.toString();
	} catch (Exception e) {
	    logger.error("IOException in displaying hdfs file " + path + ": "
		    + e.toString() + "\nStacktrace\n"
		    + e.getStackTrace().toString());
	}

	return null;
    }

    public String read() {
	String msg = "";
	try {
	    if (ifstream == null)
		setInputFile();
	    msg = ifstream.readUTF();
	} catch (Exception e) {
	    logger.error("IOException in reading hdfs file " + path + ": "
		    + e.toString() + "\nStacktrace\n"
		    + e.getStackTrace().toString());
	}

	return msg;
    }

    private void setInputFile() throws Exception {
	ifstream = fileSystem.open(destination);
    }

    // Currently for internal testing only.
    private void displayCurrentFile() {
	while (true) {
	    String str = readLine();
	    if (str != null)
		System.out.println(str);
	    else
		break;
	}
	System.out.println("");
    }

    // For internal testing only.
    private void readCurrentFile() throws Exception {
	ifstream.seek(0);
	BufferedReader bufferIn = new BufferedReader(new InputStreamReader(
		ifstream));
	char[] buf = new char[1024];
	int bytes = 0;
	int offset = 0;
	while ((bytes = bufferIn.read(buf, offset, 1024)) >= 0) {
	    System.out.println("read bytes: " + bytes);
	    String msg = new String(buf);
	    System.out.print(buf.toString());
	    // offset += bytes;
	}
    }

    public static void testStreaming(HDFSSink writter) throws Exception {
	if (writter == null)
	    return;
	
	for (int i = 0; i < 60; i++) {
	    String msg = "Message line " + i;
	    System.out.println("Sending msg: " + msg);
	    writter.send(msg, "HdfsIO main", "HDFS IO", "localhost",
			999888);
	    Thread.sleep(10000);
	}
    }

    public static void testCreating(HDFSSink writter) throws Exception {

	if (writter == null)
	    return;

		String msg = "Hello, splunker! I'm here.\n";
		writter.write(msg, "HdfsIO main", "HDFS IO", "localhost",
			999888);
		writter.close();
		Thread.sleep(1000); // sleep for 1000 ms.
		// writter.setCurrentFile();

		System.out.println("Reading file "
			+ writter.getCurrentFileName());
		writter.openRead();
		writter.displayCurrentFile();

    }

    public static void testAppending(HDFSSink writter) throws Exception {
	if (writter == null)
	    return;

	String msg = "Some message here.";

	System.out.println("Appending " + msg);
	writter.write(msg, "HdfsIO main", "HDFS IO", "localhost", 999888);

	Thread.sleep(30000);

	msg = "More message here.";

	System.out.println("Appending " + msg);
	writter.write(msg, "HdfsIO main", "HDFS IO", "localhost", 999888);

	Thread.sleep(30000);

	msg = "Even more message here.";

	System.out.println("Appending " + msg);
	writter.write(msg, "HdfsIO main", "HDFS IO", "localhost", 999888);

		// System.out
		// .print("Reading file " + writter.getCurrentFileName());
		// writter.close();
		// Thread.sleep(1000); // sleep for 1000 ms.
		// writter.openRead();
		// writter.displayCurrentFile();
    }

    public static void testReading(HDFSSink writter) throws IOException {
	if (writter == null)
	    return;

	try {
	    writter.displayCurrentFile();

	} catch (Exception ex) {
	    ex.printStackTrace();
	}
    }

    public static void main(String[] args) throws IOException {
	HDFSSink writter = null;
	boolean simulating = false; // simulating stream input to hdfs.
	boolean appending = false;
	boolean reading = false;
	boolean creating = false;
	String filename = "/xli/testmsg.txt";

	if (args.length > 0) {
	    if (args[0].contains("appending")) {
		appending = true;
		System.out.println("Test appending...");
	    } else if (args[0].contains("creating")) {
		creating = true;
		System.out.println("Test creating...");
	    } else if (args[0].contains("reading")) {
		reading = true;
		System.out.println("Test reading...");
	    } else if (args[0].contains("simulating")) {
		simulating = true;
		System.out.println("Running simulation...");
	    }

	    if (args.length > 1)
		filename = args[1];
	}

	try {
	    writter = new HDFSSink("localhost", "9000");

	    if (creating) {
		writter.openToCreate(filename);
		testCreating(writter);
	    }

	    if (reading) {
		System.out.println("Reading file " + filename);
		writter.openToRead(filename);
		testReading(writter);
	    }

	    if (appending) {
		writter.openToAppend(filename);
		testAppending(writter);
	    }

	    if (simulating) {
		writter.openToCreate(filename);
		testStreaming(writter);
	    }

	} catch (Exception ex) {
	    ex.printStackTrace();
	} finally {
	    writter.close();
	}
    }
}
