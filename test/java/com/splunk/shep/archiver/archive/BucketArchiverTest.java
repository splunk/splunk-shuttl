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
    private ArchiveBucketTransferer archiveBucketTransferer;

    @BeforeMethod(groups = { "fast-unit" })
    public void setUp() {
	config = mock(ArchiveConfiguration.class);
	exporter = mock(BucketExporter.class);
	pathResolver = mock(PathResolver.class);
	archiveBucketTransferer = mock(ArchiveBucketTransferer.class);
	bucketArchiver = new BucketArchiver(config, exporter, pathResolver,
		archiveBucketTransferer);

	bucket = mock(Bucket.class);
    }

    @Test(groups = { "fast-unit" })
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
	verify(exporter).exportBucketToFormat(bucket, format);
    }

    public void archiveBucket_shouldResolveArchivePathWithIndexBucketAndFormat() {
	BucketFormat format = BucketFormat.SPLUNK_BUCKET;
	when(config.getArchiveFormat()).thenReturn(format);
	when(
		exporter.exportBucketToFormat(eq(bucket),
			any(BucketFormat.class))).thenReturn(bucket);
	bucketArchiver.archiveBucket(bucket);
	verify(pathResolver).resolveArchivePath(bucket);
    }

    public void archiveBucket_givenArchiveBucketTransferer_letTransfererTransferTheBucket() {
	URI path = getTestUri();
	when(
		exporter.exportBucketToFormat(eq(bucket),
			any(BucketFormat.class))).thenReturn(bucket);
	when(pathResolver.resolveArchivePath(bucket)).thenReturn(path);
	bucketArchiver.archiveBucket(bucket);
	verify(archiveBucketTransferer).transferBucketToArchive(bucket, path);
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
