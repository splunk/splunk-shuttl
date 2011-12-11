package com.splunk.shep.customsearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSOut {

    public static void main(String args[]) {
	try {
	    java.io.BufferedReader stdin = new java.io.BufferedReader(
		    new java.io.InputStreamReader(System.in));

	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
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
		    out.writeUTF(arg);
		    out.writeUTF("\n");
		}
	    }
	    out.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}
