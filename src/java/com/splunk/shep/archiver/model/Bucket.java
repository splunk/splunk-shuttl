package com.splunk.shep.archiver.model;

import java.io.File;
import java.io.FileNotFoundException;

import com.splunk.shep.archiver.archive.BucketFormat;

/**
 * Model representing a Splunk bucket
 */
public class Bucket {

    private final String index;
    private final BucketFormat format;

    /**
     * Bucket with an index and format<br/>
     * Use static method {@link Bucket#createWithAbsolutePath(String)} to create
     * a bucket out of an absolute path.
     * 
     * @param index
     *            the bucket came from
     * @param format
     *            the bucket is in
     */
    public Bucket(String index, BucketFormat format) {
	this.index = index;
	this.format = format;
    }

    public BucketFormat getFormat() {
	return format;
    }

    public String getIndex() {
	return index;
    }

    public static Bucket createWithAbsolutePath(String path)
	    throws FileNotFoundException, FileNotDirectoryException {
	File directory = new File(path);
	if (!directory.exists()) {
	    throw new FileNotFoundException();
	} else if (!directory.isDirectory()) {
	    throw new FileNotDirectoryException();
	} else {
	    String index = directory.getParentFile().getParentFile().getName();
	    File rawdata = new File(directory, "rawdata");
	    BucketFormat format;
	    if (rawdata.exists()) {
		format = BucketFormat.SPLUNK_BUCKET;
	    } else {
		format = BucketFormat.UNKNOWN;
	    }
	    return new Bucket(index, format);
	}
    }

}
