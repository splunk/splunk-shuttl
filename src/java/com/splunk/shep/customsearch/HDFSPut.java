package com.splunk.shep.customsearch;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSPut {

    public static void main(String args[]) {
	try {
	    java.io.BufferedReader stdin = new java.io.BufferedReader(
		    new java.io.InputStreamReader(System.in));
	    Configuration conf = new Configuration();
	    URI fileuri = URI.create(args[0]);
	    String host = fileuri.getHost();
	    int port = fileuri.getPort();
	    FileSystem fs = FileSystem.get(
		    URI.create("hdfs://" + host + ":" + port), conf);
	    Path filenamePath = new Path(args[0]);
	    if (fs.exists(filenamePath)) {
		// remove the file first 
		fs.delete(filenamePath);
	    }
	    FSDataOutputStream out = fs.create(filenamePath);
	    while (true) {
		String arg = stdin.readLine();
		if (arg.equals("exit")) {
		    break;
		} else {
		    if (args[1].equals("csv")) {
			out.writeBytes(arg);
			out.writeBytes("\n");
		    } else {
			out.writeUTF(arg);
		    }
		}
	    }
	    out.close();
	} catch (Exception e) {
	    e.printStackTrace();
	    System.exit(1);
	}
	System.exit(0);
    }
}
