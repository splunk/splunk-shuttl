package com.splunk.shep.archiver.fileSystem;

/**
 * Describes a path on an file system. Uses the unix like path format:
 * "/This/is/a/path"
 * 
 * The paths are immutable.
 */
public class FileSystemPath {

    private final String pathString;

    public static final String SEPERATOR = "/";

    public FileSystemPath(String pathAsString) {
	pathString = pathAsString;
    }
    
    public String getPathAsString() {
	return pathString;
    }

    @Override
    public String toString() {
	return getPathAsString();
    }
}
