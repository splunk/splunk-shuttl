package com.splunk.shep.archiver.archive;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;

@Test(groups = { "fast" })
public class BucketExporterTest {

    // TODO, when Bucket is implemented, change this test to use the real bucket
    // class instead of mocking it.
    public void getBucketExportedToFormat_whenBucketIsAlreadyInThatFormat_returnTheSameBucket() {
	ArchiveFormat format = ArchiveFormat.SPLUNK_BUCKET;
	Bucket bucket = mock(Bucket.class);
	when(bucket.getFormat()).thenReturn(format);
	Bucket exportedToFormat = new BucketExporter()
		.getBucketExportedToFormat(bucket, format);
	assertEquals(bucket, exportedToFormat);
    }
}
