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
    private final String indexName;

    /**
     * Bucket with an index and format<br/>
     * 
     * @param indexName
     *            The name of the index that this bucket belongs to.
     * @param directory
     *            that is the bucket
     * @throws FileNotFoundException
     *             if the file doesn't exist
     * @throws FileNotDirectoryException
     *             if the file is not a directory
     */
    public Bucket(String indexName, File directory)
	    throws FileNotFoundException,
	    FileNotDirectoryException {
	verifyExistingDirectory(directory);
	this.directory = directory;
	this.indexName = indexName;
    }

    /**
     * Invokes #Bucket(String, File), by creating a file from specified
     * bucketPath
     */
    public Bucket(String indexName, String bucketPath)
	    throws FileNotFoundException, FileNotDirectoryException {
	this(indexName, new File(bucketPath));
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

    /**
     * @return The directory that this bucket has its data in.
     */
    public File getDirectory() {
	return directory;
    }

    /**
     * @return The name of this bucket.
     */
    public String getName() {
	return directory.getName();
    }

    /**
     * @return The name of the index that this bucket belong to.
     */
    public String getIndex() {
	return indexName;
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
	File newName = new File(directoryToMoveTo.getAbsolutePath(),
		directory.getName());

	if (!directory.renameTo(newName)) {
	    throw new RuntimeException("Can't move bucket on this file system");
	}

	return new Bucket(getIndex(), newName);
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
