package com.splunk.shep.archiver.model;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

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
	verifyExistingDirectory(directory);
	this.directory = directory;
    }

    private void verifyExistingDirectory(File directory)
	    throws FileNotFoundException, FileNotDirectoryException {
	if (!directory.exists()) {
	    throw new FileNotFoundException("Could not find directory: "
		    + directory);
	} else if (!directory.isDirectory()) {
	    throw new FileNotDirectoryException("Directory " + directory
		    + " is not a directory");
	}
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

    public Bucket moveBucketToDir(File directoryToMoveTo)
	    throws FileNotFoundException, FileNotDirectoryException {
	verifyExistingDirectory(directoryToMoveTo);
	File newBucketDir = createNewBucketPathInDirectory(directoryToMoveTo);
	verifyExistingDirectory(newBucketDir);
	moveTheContentsOfThisBucketToNewBucket(newBucketDir);
	return new Bucket(newBucketDir);
    }

    private void moveTheContentsOfThisBucketToNewBucket(File newBucketDir) {
	if (!directory.renameTo(newBucketDir))
	    throw new RuntimeException("could not rename : " + directory
		    + " to " + newBucketDir);
    }

    private File createNewBucketPathInDirectory(File newDirectory) {
	String bucketPathInformation = getIndex() + File.separator + getDB()
		+ File.separator + getName();
	File newBucketDir = new File(newDirectory, bucketPathInformation);
	newBucketDir.mkdirs();
	return newBucketDir;
    }

    public static Bucket createWithAbsolutePath(String path)
	    throws FileNotFoundException, FileNotDirectoryException {
	File directory = new File(path);
	return new Bucket(directory);
    }

    /**
     * Deletes the bucket from the file system.
     * 
     * @throws IOException
     *             if it's not possible to delete the directory
     */
    public void deleteBucket() throws IOException {
	FileUtils.deleteDirectory(getDirectory());
    }

}
