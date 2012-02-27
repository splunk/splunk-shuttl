package com.splunk.shep.testutil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * All the utils regarding hadoop Path object goes in here. If there are
 * exceptions while doing any operations the tests will fail with appropriate
 * message.
 */
public class UtilPath {

    /**
     * Same as calling {@link #getSafeDirectory(FileSystem, Class)} with
     * MethodCallerHelper.getCallerToMyMethod() as the class parameter
     * 
     * @see #getSafeDirectory(FileSystem, Class)
     */
    public static Path getSafeDirectory(FileSystem fileSystem) {
	return getSafeDirectory(fileSystem,
		MethodCallerHelper.getCallerToMyMethod());
    }

    /**
     * SafePathCreator is used to get a directory in a file system which is
     * class unique, readable and writable.
     * 
     * It returns a path like this: /User/XXX/org.shep.HadoopTest/
     * 
     */
    public static Path getSafeDirectory(FileSystem fileSystem, Class<?> clazz) {
	return new Path(fileSystem.getHomeDirectory() + "/" + clazz.getName());
    }
}