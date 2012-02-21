package com.splunk.shep.customsearch;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSCat {

    /**
     * @param args
     *            [0] containing a path to a file on a hadoop filesystem.
     */
    public static void main(String args[]) {
	Configuration conf = new Configuration();
	FileSystem fs;
	Path filenamePath = new Path(args[0]);
	URI fileuri = URI.create(args[0]);

	fs = getFileSystemDepedningOnURISchema(conf, fileuri);
	verifyFileExistance(filenamePath, fs);
	printContentsOfPathToSystemOut(fs, filenamePath);
	System.exit(0);
    }

    private static FileSystem getFileSystemDepedningOnURISchema(
	    Configuration conf, URI fileuri) {
	try {
	    return doGetFileSystem(conf, fileuri);
	} catch (IOException e) {
	    e.printStackTrace();
	    System.exit(2);
	    return null; // Won't happen, because of System.exit.
	}
    }

    private static FileSystem doGetFileSystem(Configuration conf, URI fileuri)
	    throws IOException {
	FileSystem fs;
	if (fileuri.getScheme().equals("file")) {
	    fs = FileSystem.getLocal(conf);
	} else {
	    String host = fileuri.getHost();
	    int port = fileuri.getPort();
	    fs = FileSystem
		    .get(URI.create("hdfs://" + host + ":" + port), conf);
	}
	return fs;
    }

    private static void verifyFileExistance(Path filenamePath, FileSystem fs) {
	if (!isFileExisting(filenamePath, fs)) {
	    System.exit(1);
	}
    }

    private static boolean isFileExisting(Path filenamePath, FileSystem fs) {
	try {
	    return fs.exists(filenamePath);
	} catch (IOException e) {
	    e.printStackTrace();
	    return false;
	}
    }

    private static void printContentsOfPathToSystemOut(FileSystem fs,
	    Path filenamePath) {
	try {
	    doPrintContents(fs, filenamePath);
	} catch (IOException e) {
	    e.printStackTrace();
	    System.exit(2);
	}
    }

    private static void doPrintContents(FileSystem fs, Path filenamePath)
	    throws IOException {
	FSDataInputStream in = fs.open(filenamePath);
	while (true) {
	    String line = in.readLine();
	    if (line == null) {
		break;
	    }
	    System.out.println(line);
	}
	in.close();
    }
}
