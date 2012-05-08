package com.splunk.shep.archiver.archive;

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.File;

import org.apache.log4j.Logger;

import com.splunk.shep.archiver.model.Bucket;

/**
 * For exporting a {@link Bucket} to {@link BucketFormat#CSV}, since it's
 * currently the only other format to export to.
 */
public class BucketExporter {

    private final static Logger logger = Logger.getLogger(BucketExporter.class);
    private final CsvExporter csvExporter;
    private final CsvBucketCreator csvBucketCreator;

    /**
     * @param csvExporter
     *            for exporting the bucket to a .csv file.
     * @param csvBucketCreator
     *            for creating a {@link Bucket} from the .csv file.
     */
    public BucketExporter(CsvExporter csvExporter,
	    CsvBucketCreator csvBucketCreator) {
	this.csvExporter = csvExporter;
	this.csvBucketCreator = csvBucketCreator;
    }

    /**
     * @return a new {@link Bucket} in the new format.
     */
    public Bucket exportBucketToFormat(Bucket bucket, BucketFormat newFormat) {
	if (newFormat.equals(BucketFormat.UNKNOWN)) {
	    logUnknownBucketException(bucket, newFormat);
	    throw new UnknownBucketFormatException();
	}

	if (isSameFormat(bucket, newFormat)) {
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

    private boolean isSameFormat(Bucket bucket, BucketFormat newFormat) {
	return bucket.getFormat().equals(newFormat);
    }

    private Bucket getBucketInNewFormat(Bucket bucket, BucketFormat newFormat) {
	if (bucket.getFormat().equals(BucketFormat.SPLUNK_BUCKET)
		&& newFormat.equals(BucketFormat.CSV)) {
	    return getBucketInCsvFormat(bucket);
	} else {
	    throw new UnsupportedOperationException();
	}
    }

    private Bucket getBucketInCsvFormat(Bucket bucket) {
	File csvFile = csvExporter.exportBucketToCsv(bucket);
	return csvBucketCreator.createBucketWithCsvFile(csvFile, bucket);
    }

    /**
     * @return an instance of the {@link BucketExporter}
     */
    public static BucketExporter create() {
	CsvExporter csvExporter = CsvExporter.create();
	return new BucketExporter(csvExporter, new CsvBucketCreator());
    }

}
