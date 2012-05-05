package com.splunk.shep.archiver.archive;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketExporterTest {

    BucketExporter bucketExporter;
    SplunkExportTool splunkExportTool;

    @BeforeMethod(groups = { "fast-unit" })
    public void setUp() {
	splunkExportTool = mock(SplunkExportTool.class);
	bucketExporter = new BucketExporter(splunkExportTool);
    }

    @Test(groups = { "fast-unit" })
    public void exportBucketToFormat_whenBucketIsAlreadyInThatFormat_returnTheSameBucket() {
	BucketFormat format = BucketFormat.SPLUNK_BUCKET;
	Bucket bucket = mock(Bucket.class);
	when(bucket.getFormat()).thenReturn(format);
	Bucket exportedToFormat = bucketExporter.exportBucketToFormat(bucket,
		format);
	assertEquals(bucket, exportedToFormat);
    }

    @Test(expectedExceptions = { UnknownBucketFormatException.class })
    public void exportBucketToFormat_formatIsUnknown_throwUnknownBucketFormatException() {
	Bucket bucket = mock(Bucket.class);
	bucketExporter.exportBucketToFormat(bucket, BucketFormat.UNKNOWN);
    }

    public void exportBucketToFormat_bucketIsInSplunkBucketAndExportingToCsv_callsExportToolWithBucket() {
	Bucket bucket = UtilsBucket.createTestBucket();
	assertEquals(BucketFormat.SPLUNK_BUCKET, bucket.getFormat());
	Bucket csvBucket = mock(Bucket.class);
	when(splunkExportTool.exportToCsv(bucket)).thenReturn(csvBucket);
	Bucket actualBucket = bucketExporter.exportBucketToFormat(bucket,
		BucketFormat.CSV);
	assertSame(csvBucket, actualBucket);
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
	verifyZeroInteractions(splunkExportTool);
    }
}
