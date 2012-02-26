package com.splunk.shep.archiver.archive;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.fileSystem.FileSystemPath;
import com.splunk.shep.archiver.model.Bucket;

@Test(groups = { "fast" })
public class BucketArchiverTest {

    private BucketArchiver bucketArchiver;
    private ArchiveConfiguration config;
    private BucketExporter exporter;

    private PathResolver pathResolver;
    private Bucket bucket;
    private BucketTransferer bucketTransferer;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	config = mock(ArchiveConfiguration.class);
	exporter = mock(BucketExporter.class);
	pathResolver = mock(PathResolver.class);
	bucketTransferer = mock(BucketTransferer.class);
	bucketArchiver = new BucketArchiver(config, exporter, pathResolver,
		bucketTransferer);

	bucket = new Bucket();
    }

    public void archiveBucket_shouldGetArchiveFormat() {
	bucketArchiver.archiveBucket(bucket);
	verify(config).getArchiveFormat();
    }

    public void archiveBucket_shouldUseTheArchiveFormatForExportingTheBucket() {
	// Setup
	ArchiveFormat format = ArchiveFormat.SPLUNK_BUCKET;
	when(config.getArchiveFormat()).thenReturn(format);
	// Test
	bucketArchiver.archiveBucket(bucket);
	// Verification
	verify(exporter).exportBucketToFormat(bucket, format);
    }

    public void archiveBucket_shouldResolveArchivePathWithIndexBucketAndFormat() {
	ArchiveFormat format = ArchiveFormat.SPLUNK_BUCKET;
	when(config.getArchiveFormat()).thenReturn(format);
	bucketArchiver.archiveBucket(bucket);
	verify(pathResolver).resolveArchivePathWithBucketAndFormat(bucket,
		format);
    }

    public void archiveBucket_shouldLetBucketSender_TransferTheBucket() {
	FileSystemPath path = new FileSystemPath("some/path");
	when(
		pathResolver.resolveArchivePathWithBucketAndFormat(eq(bucket),
			any(ArchiveFormat.class))).thenReturn(path);
	bucketArchiver.archiveBucket(bucket);
	verify(bucketTransferer).transferBucketToPath(bucket, path);
    }
}
