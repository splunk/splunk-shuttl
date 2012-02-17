package com.splunk.shep.testutil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SafePathCreator {

    private final MethodCallerHelper methodCallerHelper;

    public SafePathCreator(MethodCallerHelper methodCallerHelper) {
	this.methodCallerHelper = methodCallerHelper;
    }

    public Path getPathOnFileSystem(FileSystem fileSystem) {
	StackTraceElement caller = methodCallerHelper.getCallerToMyMethod();
	return new Path(fileSystem.getHomeDirectory() + "/"
		+ caller.getClassName());
    }

    public static SafePathCreator get() {
	return new SafePathCreator(new MethodCallerHelper());
    }

}
