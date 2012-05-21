package com.splunk.shuttl.testutil;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class FileSystemUtils {

    public static FileSystem getLocalFileSystem() {
	Configuration configuration = new Configuration();
	try {
	    return FileSystem.getLocal(configuration);
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    public static FileSystem getRemoteFileSystem(String host, String port) {
        Configuration conf = new Configuration();
        try {
            URI hdfsUri = new URI("hdfs", null, host, Integer.parseInt(port), null, null, null);
            return FileSystem.get(hdfsUri, conf);
	} catch (IOException e) {
	    throw new RuntimeException(e);
	} catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
