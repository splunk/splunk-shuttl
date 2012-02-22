package com.splunk.shep.testutil;

import java.io.IOException;

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
}
