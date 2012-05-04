package com.splunk.shep.archiver.archive;

import java.io.File;

public enum BucketFormat {
    SPLUNK_BUCKET, UNKNOWN, CSV;

    /**
     * @param directory
     *            to a bucket
     * @return format depending on what is in the bucket directory.
     */
    public static BucketFormat getFormatFromDirectory(File directory) {
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
