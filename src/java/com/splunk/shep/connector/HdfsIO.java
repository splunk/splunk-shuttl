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

package com.splunk.shep.connector;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HdfsIO implements DataSink {
    private String ip = new String("localhost");
    private String port = new String("50001");
    private String path = new String("xli");

    private Configuration conf = new Configuration();
    private FileSystem fileSystem = null;
    private Path destination = null;
    private FSDataOutputStream ofstream = null;

    private long totalBytesWritten = 0; // total bytes with current connection.
    private long fileRollingSize = 10000000;
    private long maxEventSize = 32000; // not supported for now.

    private static Logger logger = Logger.getLogger(HdfsIO.class);

    public HdfsIO() throws Exception {
	init();
    }

    public HdfsIO(String targetIP, String targetPort, String filePath)
	    throws Exception {
	port = targetPort;
	ip = targetIP;
	path = filePath;
	init();
    }

    public HdfsIO(String targetIP, String targetPort) throws Exception {
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
	    String tarURL = new String("hdfs://") + ip + ':' + port + path
		    + System.currentTimeMillis();
	    destination = new Path(tarURL);

	    if (fileSystem.exists(destination)) {
		ofstream = fileSystem.append(destination);
		logger.info("Append to: " + tarURL);
	    } else {
		ofstream = fileSystem.create(destination);
		logger.info("Overwrite: " + tarURL);
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	    logger.error("Exception in setting Hadoop connection: "
		    + e.toString());
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
	    String tarURL = new String("hdfs://") + ip + ':' + port + path
		    + System.currentTimeMillis();
	    destination = new Path(tarURL);

	    if (!fileSystem.exists(destination)) {
		return false;
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	    logger.error("Exception in setting Hadoop fiel path: "
		    + e.toString());
	}
	return true;
    }

    public void setFileRollingSize(long size) {
	fileRollingSize = size;
	logger.info("HDFS file rolling size: " + fileRollingSize);
    }

    public void setMaxEventSize(long size) {
	maxEventSize = size;
	logger.info("Ignored max event size: " + maxEventSize);
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

    public void close() {
	if (ofstream == null)
	    return;

	try {
	    ofstream.close();
	    ofstream = null;
	    logger.info("closed: " + path + " with size " + totalBytesWritten);
	    totalBytesWritten = 0;
	} catch (Exception e) {
	    e.printStackTrace();
	    logger.error("Exception in closing Hadoop connection: "
		    + e.toString());
	}
    }

    private void checkFileSize() {
	if (totalBytesWritten < fileRollingSize) {
	    if (totalBytesWritten > 0)
		logger.debug("Total bytes written so far: " + totalBytesWritten);

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

    public long getFileModTime() throws Exception {
	if (destination == null)
	    return -1;
	if (fileSystem.exists(destination)) {
	    FileStatus fileStatus = fileSystem.getFileStatus(destination);
	    return fileStatus.getModificationTime();
	}
	return -1;
    }

    private void deleteCurrentFile() {
	close();

	try {
	    if (fileSystem.exists(destination)) {
		// remove the file.
		fileSystem.delete(destination);
	    }

	    logger.info("deleted hdfs file: " + path);
	} catch (Exception e) {
	    e.printStackTrace();
	    logger.error("Exception in deleting Hdfs file: " + path + " - "
		    + e.toString());
	}
    }

    public void send(byte[] rawBytes, String sourceType, String source,
	    String host, long time) throws Exception {
	String msg = new String(rawBytes);
	write(msg, sourceType, source, host, time);
	checkFileSize();
    }

    public void write(byte[] rawBytes, String sourceType, String source,
	    String host, long time) throws Exception {
	String message = HdfsEvent.build(rawBytes, sourceType, source, host,
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

	try {
	    ofstream.writeUTF(message);
	    ofstream.flush();
	    logger.debug("Sent 1 event to Hadoop.");
	    totalBytesWritten += message.length();
	} catch (IOException e) {
	    logger.info("Sending " + message);
	    e.printStackTrace();
	    logger.error("IOException during sending: " + e.toString());
	}
    }

    public void send(String data, String sourceType, String source,
	    String host, long time) throws Exception {
	write(data, sourceType, source, host, time);
	checkFileSize();
    }

    public void write(String data, String sourceType, String source,
	    String host, long time) throws Exception {
	String message = HdfsEvent.build(data, sourceType, source, host, time);
	logger.debug("Sending " + message);

	if (ofstream == null) {
	    logger.warn("Cannot write: need to set connection.");
	    start();

	    if (ofstream == null) {
		logger.error("Failed writing data: connection not set.");
		return;
	    }
	}

	try {
	    ofstream.writeUTF(message);
	    ofstream.flush();
	    logger.debug("Sent 1 event to Hadoop.");
	    totalBytesWritten += message.length();
	} catch (IOException e) {
	    logger.info("Sending " + message);
	    e.printStackTrace();
	    logger.error("IOException during sending: " + e.toString());
	}
    }

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

    public String read(String fileNamePath) throws Exception {
	String file = "";
	if (fileNamePath == null)
	    return file;

	try {
	    file = fileNamePath;
	    if (file.charAt(0) != '/')
		file = "/" + fileNamePath;

	    String tarURL = new String("hdfs://") + ip + ':' + port + file;
	    Path dest = new Path(tarURL);

	    return read(dest);
	} catch (Exception e) {
	    logger.error("IOException in reading hdfs file " + file + ": "
		    + e.toString());
	    throw (e);
	}
    }

    // Read current file.
    public String read() {
	String msg = "";

	try {
	    msg = read(destination);
	} catch (Exception e) {
	    e.printStackTrace();
	    logger.error("IOException in displaying hdfs file " + path + ": "
		    + e.toString());
	}

	return msg;
    }

    private String read(Path dest) throws Exception {
	FSDataInputStream in = fileSystem.open(dest);
	String messageIn = in.readUTF();
	in.close();
	return messageIn;
    }

    public void displayCurrentFile() {
	System.out.print(read());
    }

    public static void main(String[] args) throws IOException {
	String msg = "Hello, splunker! I'm here.\n";
	HdfsIO writter = null;

	try {
	    writter = new HdfsIO();
	    writter.start("/xli/test.txt");
	    writter.write(msg, "testsrc", "test", "localhost", 999888);
	    System.out.print(writter.read());
	} catch (Exception ex) {
	    ex.printStackTrace();
	} finally {
	    writter.close();
	}
    }
}
