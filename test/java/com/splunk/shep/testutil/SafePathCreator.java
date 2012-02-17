package com.splunk.shep.testutil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * SafePathCreator is used to get a directory in a file system which is class
 * unique, readable and writable.
 * 
 * @author periksson@splunk.com
 * 
 */
public class SafePathCreator {

    public Path getSafeDirectory(FileSystem fileSystem, Class<?> clazz) {
	return new Path(fileSystem.getHomeDirectory() + "/" + clazz.getName());
    }

    public static SafePathCreator get() {
	return new SafePathCreator();
    }

}
