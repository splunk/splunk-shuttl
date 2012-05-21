package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.testutil.UtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketExporter;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.CsvBucketCreator;
import com.splunk.shuttl.archiver.archive.CsvExporter;
import com.splunk.shuttl.archiver.archive.UnknownBucketFormatException;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.UtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketExporterTest {

    BucketExporter bucketExporter;
    CsvExporter csvExporter;
    CsvBucketCreator csvBucketCreator;

    @BeforeMethod(groups = { "fast-unit" })
    public void setUp() {
	csvExporter = mock(CsvExporter.class);
	csvBucketCreator = mock(CsvBucketCreator.class);
	bucketExporter = new BucketExporter(csvExporter, csvBucketCreator);
    }

    @Test(groups = { "fast-unit" })
    public void exportBucketToFormat_whenBucketIsAlreadyInThatFormat_returnTheSameBucket() {
	Bucket bucket = mock(Bucket.class);
	when(bucket.getFormat()).thenReturn(BucketFormat.SPLUNK_BUCKET);
	Bucket exportedToFormat = bucketExporter.exportBucketToFormat(bucket,
		BucketFormat.SPLUNK_BUCKET);
	assertSame(bucket, exportedToFormat);
    }

    @Test(expectedExceptions = { UnknownBucketFormatException.class })
    public void exportBucketToFormat_formatIsUnknown_throwUnknownBucketFormatException() {
	Bucket bucket = mock(Bucket.class);
	bucketExporter.exportBucketToFormat(bucket, BucketFormat.UNKNOWN);
    }

    public void exportBucketToFormat_exportsSplunkBucketWithCsvExporter_createsAndReturnsBucketFromCsvFile() {
	Bucket bucket = UtilsBucket.createTestBucket();
	File csvFile = createTestFile();
	when(csvExporter.exportBucketToCsv(bucket)).thenReturn(csvFile);
	Bucket csvBucket = mock(Bucket.class);
	when(csvBucketCreator.createBucketWithCsvFile(csvFile, bucket))
		.thenReturn(csvBucket);
	Bucket newBucket = bucketExporter.exportBucketToFormat(bucket,
		BucketFormat.CSV);
	assertEquals(csvBucket, newBucket);
    }

    public void exportBucketToFormat_bucketIsUnknownAndExportingToCsv_throwsUnsupportedOperationException() {
	Bucket unknownFormatedBucket = mock(Bucket.class);
	when(unknownFormatedBucket.getFormat())
		.thenReturn(BucketFormat.UNKNOWN);
	try {
	    bucketExporter.exportBucketToFormat(unknownFormatedBucket,
		    BucketFormat.CSV);
	    fail();
	} catch (UnsupportedOperationException e) {
	}
	verifyZeroInteractions(csvExporter);
    }
}
