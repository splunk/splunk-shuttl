package com.splunk.shep.archiver.archive;

import com.splunk.shep.archiver.model.Bucket;

public class BucketExporter {

    public Bucket getBucketExportedToFormat(Bucket bucket, BucketFormat format) {
	if (format.equals(BucketFormat.UNKNOWN)) {
	    throw new UnknownBucketFormatException();
	}
	if (bucket.getFormat().equals(format)) {
	    return bucket;
	} else {
	    throw new UnsupportedOperationException();
	}
    }

}
