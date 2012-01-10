package com.splunk.shep.customsearch;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSCat {

    public static void main(String args[]) {
	try {
	    Configuration conf = new Configuration();
	    URI fileuri = URI.create(args[0]);
	    String host = fileuri.getHost();
	    int port = fileuri.getPort();
	    FileSystem fs = FileSystem.get(
		    URI.create("hdfs://" + host + ":" + port), conf);
	    Path filenamePath = new Path(args[0]);
	    if (!fs.exists(filenamePath)) {
		System.exit(1);
	    }
	    FSDataInputStream in = fs.open(filenamePath);
	    while (true) {
		String line = in.readLine();
		if (line == null) {
		    break;
		}
		System.out.println(line);
	    }
	    in.close();
	} catch (Exception e) {
	    e.printStackTrace();
	    System.exit(2);
	}
	System.exit(0);
    }
}
