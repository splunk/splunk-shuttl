package com.splunk.shep.archiver.model;

import java.io.File;
import java.io.FileNotFoundException;

import com.splunk.shep.archiver.archive.ArchiveFormat;

/**
 * Model representing a Splunk bucket
 */
public class Bucket {

    private final String index;

    protected Bucket(String index) {
	this.index = index;
    }

    public ArchiveFormat getFormat() {
	throw new UnsupportedOperationException();
    }

    public String getIndex() {
	return index;
    }

    public static Bucket createWithAbsolutePath(String path)
	    throws FileNotFoundException {
	File directory = new File(path);
	if (!directory.exists()) {
	    throw new FileNotFoundException();
	} else {
	    String index = directory.getParentFile().getParentFile().getName();
	    return new Bucket(index);
	}
    }

}
