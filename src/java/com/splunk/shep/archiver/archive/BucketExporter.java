package com.splunk.shep.archiver.archive;

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.File;

import org.apache.log4j.Logger;

import com.splunk.shep.archiver.model.Bucket;

/**
 * For changing the {@link BucketFormat} of a {@link Bucket}
 */
public class BucketExporter {

    private final static Logger logger = Logger.getLogger(BucketExporter.class);
    private final CsvExporter csvExporter;

    /**
     * @param csvExporter
     *            for exporting the bucket to csv format.
     */
    public BucketExporter(CsvExporter csvExporter) {
	this.csvExporter = csvExporter;
    }

    /**
     * @return a new {@link Bucket} in the new format.
     */
    public Bucket exportBucketToFormat(Bucket bucket, BucketFormat newFormat) {
	if (newFormat.equals(BucketFormat.UNKNOWN)) {
	    logUnknownBucketException(bucket, newFormat);
	    throw new UnknownBucketFormatException();
	}

	if (bucket.getFormat().equals(newFormat)) {
	    return bucket;
	} else {
	    return getBucketInNewFormat(bucket, newFormat);
	}
    }

    private void logUnknownBucketException(Bucket bucket, BucketFormat newFormat) {
	logger.debug(warn("Attempted to export bucket to newFormat",
		"Bucket was in an Unknown format", "Throwing exception",
		"bucket", bucket, "new_format", newFormat));
    }

    private Bucket getBucketInNewFormat(Bucket bucket, BucketFormat newFormat) {
	if (bucket.getFormat().equals(BucketFormat.SPLUNK_BUCKET)
		&& newFormat.equals(BucketFormat.CSV)) {
	    File csvBucket = csvExporter.exportBucketToCsv(bucket);
	    return null;
	} else {
	    throw new UnsupportedOperationException();
	}
    }

}
