package com.splunk.shep.archiver.model;

import java.io.File;
import java.io.FileNotFoundException;

import com.splunk.shep.archiver.archive.BucketFormat;

/**
 * Model representing a Splunk bucket
 */
public class Bucket {

    private final String name;
    private final String index;
    private final BucketFormat format;

    /**
     * Bucket with an index and format<br/>
     * Use static method {@link Bucket#createWithAbsolutePath(String)} to create
     * a bucket out of an absolute path.
     * 
     * @param index
     *            the bucket came from
     * @param index2
     * @param format
     *            the bucket is in
     */
    public Bucket(String name, String index, BucketFormat format) {
	this.name = name;
	this.index = index;
	this.format = format;
    }

    public String getName() {
	return name;
    }

    public String getIndex() {
	return index;
    }

    public BucketFormat getFormat() {
	return format;
    }

    public static Bucket createWithAbsolutePath(String path)
	    throws FileNotFoundException, FileNotDirectoryException {
	File directory = new File(path);
	if (!directory.exists()) {
	    throw new FileNotFoundException();
	} else if (!directory.isDirectory()) {
	    throw new FileNotDirectoryException();
	} else {
	    return createBucketWithDirectory(directory);
	}
    }

    private static Bucket createBucketWithDirectory(File directory) {
	String name = directory.getName();
	String index = directory.getParentFile().getParentFile().getName();
	BucketFormat format = getFormatFromDirectory(directory);
	return new Bucket(name, index, format);
    }

    private static BucketFormat getFormatFromDirectory(File directory) {
	File rawdataInDirectory = new File(directory, "rawdata");
	BucketFormat format;
	if (rawdataInDirectory.exists()) {
	    format = BucketFormat.SPLUNK_BUCKET;
	} else {
	    format = BucketFormat.UNKNOWN;
	}
	return format;
    }

}
