package com.splunk.shep.archiver.archive;

import static com.splunk.shep.archiver.LogFormatter.*;

import org.apache.log4j.Logger;

import com.splunk.shep.archiver.model.Bucket;

public class BucketExporter {

    private final static Logger logger = Logger.getLogger(BucketExporter.class);

    public Bucket getBucketExportedToFormat(Bucket bucket,
	    BucketFormat expectedFormat) {
	if (expectedFormat.equals(BucketFormat.UNKNOWN)) {
	    throw new UnknownBucketFormatException();
	}

	BucketFormat actualFormat = bucket.getFormat();
	if (actualFormat.equals(expectedFormat)) {
	    return bucket;
	} else {
	    logger.error(did("bucket was not of the expected format",
		    "bucket had format " + actualFormat,
		    "bucket should have had format " + expectedFormat));
	    throw new UnsupportedOperationException(
		    "unexpected bucket format: " + actualFormat);
	}
    }

}
