package com.splunk.shep.archiver.archive;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsTestNG;

@Test(groups = { "fast-unit" })
public class BucketArchiverTest {

    private BucketArchiver bucketArchiver;
    private ArchiveConfiguration config;
    private BucketExporter exporter;

    private PathResolver pathResolver;
    private Bucket bucket;
    private BucketTransferer bucketTransferer;

    @BeforeMethod(groups = { "fast-unit" })
    public void setUp() {
	config = mock(ArchiveConfiguration.class);
	exporter = mock(BucketExporter.class);
	pathResolver = mock(PathResolver.class);
	bucketTransferer = mock(BucketTransferer.class);
	bucketArchiver = new BucketArchiver(config, exporter, pathResolver,
		bucketTransferer);

	bucket = mock(Bucket.class);
    }

    public void archiveBucket_shouldGetArchiveFormat() {
	bucketArchiver.archiveBucket(bucket);
	verify(config).getArchiveFormat();
    }

    public void archiveBucket_shouldUseTheArchiveFormatForExportingTheBucket() {
	// Setup
	BucketFormat format = BucketFormat.SPLUNK_BUCKET;
	when(config.getArchiveFormat()).thenReturn(format);
	// Test
	bucketArchiver.archiveBucket(bucket);
	// Verification
	verify(exporter).getBucketExportedToFormat(bucket, format);
    }

    public void archiveBucket_shouldResolveArchivePathWithIndexBucketAndFormat() {
	BucketFormat format = BucketFormat.SPLUNK_BUCKET;
	when(config.getArchiveFormat()).thenReturn(format);
	when(
		exporter.getBucketExportedToFormat(eq(bucket),
			any(BucketFormat.class))).thenReturn(bucket);
	bucketArchiver.archiveBucket(bucket);
	verify(pathResolver).resolveArchivePath(bucket);
    }

    public void archiveBucket_shouldLetBucketSender_TransferTheBucket() {
	URI path = getTestUri();
	when(
		exporter.getBucketExportedToFormat(eq(bucket),
			any(BucketFormat.class))).thenReturn(bucket);
	when(pathResolver.resolveArchivePath(bucket)).thenReturn(path);
	bucketArchiver.archiveBucket(bucket);
	verify(bucketTransferer).transferBucketToArchive(bucket, path);
    }

    private URI getTestUri() {
	try {
	    return new URI("file:/some/path");
	} catch (URISyntaxException e) {
	    UtilsTestNG.failForException("Could not create URI: ", e);
	    return null;
	}
    }
}
