package com.splunk.shep.testutil;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * All the utils regarding hadoop FileSystem object goes in here. If there are
 * exceptions while doing any operations the tests will fail with appropriate
 * message.
 */
public class UtilsFileSystem {

    /**
     * Creates a local filesystem failing the test if it can't.
     */
    public static FileSystem getLocalFileSystem() {
	Configuration configuration = new Configuration();
	try {
	    return FileSystem.getLocal(configuration);
	} catch (IOException e) {
	    UtilsTestNG.failForException("Couldn't create a local filesystem",
		    e);
	    return null; // Will not be executed.
	}
    }

    /**
     * @return The file on specified path from specified filessystem.
     */
    public static File getFileFromFileSystem(FileSystem fileSystem,
	    Path pathOftheFileOnRemote) {
	File retrivedFile = UtilsFile.createTestFilePath();
	Path localFilePath = new Path(retrivedFile.toURI());
	try {
	    fileSystem.copyToLocalFile(pathOftheFileOnRemote, localFilePath);
	} catch (IOException e) {
	    UtilsTestNG.failForException(
		    "Can't retrive the file from remote filesystem", e);
	}
	return retrivedFile;
    }

    /**
     * Puts specified file to a safe place on specified file system and return
     * the path.
     */
    public static Path putFileToFileSystem(FileSystem fileSystem, File fileToPut) {
	Path remotePath = UtilsPath.getSafeDirectory(fileSystem, MethodCallerHelper.getCallerToMyMethod());
	Path localFilePath = new Path(fileToPut.toURI());
	try {
	    fileSystem.copyFromLocalFile(localFilePath, remotePath);
	} catch (IOException e) {
	    UtilsTestNG.failForException("Can't put file to remote filesystem",
		    e);
	}
	return remotePath;
    }
}
