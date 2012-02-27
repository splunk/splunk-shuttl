package com.splunk.shep.archiver.model;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.splunk.shep.archiver.archive.BucketFormat;

/**
 * Model representing a Splunk bucket
 */
public class Bucket {

    private final File directory;

    /**
     * Bucket with an index and format<br/>
     * Use static method {@link Bucket#createWithAbsolutePath(String)} to create
     * a bucket out of an absolute path.
     * 
     * @param directory
     *            that is the bucket
     * @throws FileNotFoundException
     *             if the file doesn't exist
     * @throws FileNotDirectoryException
     *             if the file is not a directory
     */
    public Bucket(File directory) throws FileNotFoundException,
	    FileNotDirectoryException {
	if (!directory.exists()) {
	    throw new FileNotFoundException("Could not find file: " + directory);
	} else if (!directory.isDirectory()) {
	    throw new FileNotDirectoryException("File " + directory
		    + " is not a directory");
	}
	this.directory = directory;
    }

    public File getDirectory() {
	return directory;
    }

    public String getName() {
	return directory.getName();
    }

    private String getDB() {
	return directory.getParentFile().getName();
    }

    public String getIndex() {
	return directory.getParentFile().getParentFile().getName();
    }

    public BucketFormat getFormat() {
	return getFormatFromDirectory(directory);
    }

    private BucketFormat getFormatFromDirectory(File directory) {
	File rawdataInDirectory = new File(directory, "rawdata");
	BucketFormat format;
	if (rawdataInDirectory.exists()) {
	    format = BucketFormat.SPLUNK_BUCKET;
	} else {
	    format = BucketFormat.UNKNOWN;
	}
	return format;
    }

    public Bucket moveBucketToDir(File directoryToMoveTo) {
	File newBucketDir = createNewBucketPathInDirectory(directoryToMoveTo);
	File directoryCopy = directory.getAbsoluteFile();
	directoryCopy.renameTo(newBucketDir);
	return getBucketWithoutExceptions(newBucketDir);
    }

    private File createNewBucketPathInDirectory(File newDirectory) {
	String bucketPathInformation = getIndex() + File.separator + getDB()
		+ File.separator + getName();
	File newBucketDir = new File(newDirectory, bucketPathInformation);
	newBucketDir.mkdirs();
	return newBucketDir;
    }

    private Bucket getBucketWithoutExceptions(File newBucketDir) {
	try {
	    return new Bucket(newBucketDir);
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new IllegalStateException(
		    "Internal bucket call. Should be able to create it self", e);
	}
    }

    public static Bucket createWithAbsolutePath(String path)
	    throws FileNotFoundException, FileNotDirectoryException {
	File directory = new File(path);
	return new Bucket(directory);
    }

}
