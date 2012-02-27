package com.splunk.shep.archiver.model;

import java.io.File;
import java.io.FileNotFoundException;

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
	    throw new FileNotFoundException();
	} else if (!directory.isDirectory()) {
	    throw new FileNotDirectoryException();
	}
	this.directory = directory;
    }

    public File getDirectory() {
	return directory;
    }

    public String getName() {
	return directory.getName();
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

    public static Bucket createWithAbsolutePath(String path)
	    throws FileNotFoundException, FileNotDirectoryException {
	File directory = new File(path);
	return new Bucket(directory);
    }
}
