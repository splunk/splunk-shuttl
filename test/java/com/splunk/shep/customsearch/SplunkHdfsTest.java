package com.splunk.shep.customsearch;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SplunkHdfsTest {

    protected void putFileinHDFS(String uri, String msg) throws IOException {
	FileSystem fs = getFileSystem(uri);
	Path filepath = new Path(uri);
	// TODO fs.mkdirs(filepath);
	if (fs.exists(filepath)) {
	    // remove the file first
	    fs.delete(filepath);
	}
	FSDataOutputStream out = fs.create(filepath);
	out.writeBytes(msg);
	out.close();
    }

    protected FSDataInputStream getFileinHDFS(String uri) throws IOException {
	FileSystem fs = getFileSystem(uri);
	Path filepath = new Path(uri);
	if (fs.exists(filepath)) {
	    FSDataInputStream os = fs.open(filepath);
	    return os;
	}
	throw new IOException("File not found");
    }

    protected void mkdirsinHDFS(String uri) throws IOException {
	FileSystem fs = getFileSystem(uri);
	Path filepath = new Path(uri);
	fs.mkdirs(filepath);
    }

    protected void deleteFileinHDFS(String uri) throws IOException {
	FileSystem fs = getFileSystem(uri);
	Path filepath = new Path(uri);
	if (fs.exists(filepath)) {
	    // remove the file first
	    fs.delete(filepath);
	    // TODO need to delete the dir as well
	}
    }

    FileSystem getFileSystem(String uri) throws IOException {
	Configuration conf = new Configuration();
	URI fileuri = URI.create(uri);
	String host = fileuri.getHost();
	int port = fileuri.getPort();
	FileSystem fs = FileSystem.get(
		URI.create("hdfs://" + host + ":" + port), conf);
	return fs;
    }

}
