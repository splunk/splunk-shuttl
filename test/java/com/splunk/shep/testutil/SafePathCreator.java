package com.splunk.shep.testutil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * SafePathCreator is used to get a directory in a file system which is unique,
 * readable and writable.
 * 
 * @author periksson@splunk.com
 * 
 */
public class SafePathCreator {

    private final MethodCallerHelper methodCallerHelper;

    public SafePathCreator(MethodCallerHelper methodCallerHelper) {
	this.methodCallerHelper = methodCallerHelper;
    }

    public Path getSafeDirectory(FileSystem fileSystem) {
	StackTraceElement caller = methodCallerHelper.getCallerToMyMethod();
	return new Path(fileSystem.getHomeDirectory() + "/"
		+ caller.getClassName());
    }

    public static SafePathCreator get() {
	return new SafePathCreator(new MethodCallerHelper());
    }

}
