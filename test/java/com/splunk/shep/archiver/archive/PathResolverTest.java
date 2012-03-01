package com.splunk.shep.archiver.archive;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.net.URI;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.fileSystem.WritableFileSystem;
import com.splunk.shep.archiver.model.Bucket;

@Test(groups = { "fast" })
public class PathResolverTest {

    private ArchiveConfiguration configuration;
    private PathResolver pathResolver;
    private Bucket bucket;
    private WritableFileSystem writableFileSystem;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	configuration = mock(ArchiveConfiguration.class);
	writableFileSystem = mock(WritableFileSystem.class);
	pathResolver = new PathResolver(configuration, writableFileSystem);

	bucket = mock(Bucket.class);
    }

    public void resolveArchivePath_givenValidBucket_combineBucketAndConfigurationToCreateTheEndingArchivePath() {
	String archiveRoot = "archiving_root";
	when(configuration.getArchivingRoot()).thenReturn(archiveRoot);
	String clusterName = "cluster_name";
	when(configuration.getClusterName()).thenReturn(clusterName);
	String serverName = "server_name";
	when(configuration.getServerName()).thenReturn(serverName);
	String bucketName = "bucket_name_id";
	when(bucket.getName()).thenReturn(bucketName);
	String bucketIndex = "index";
	when(bucket.getIndex()).thenReturn(bucketIndex);
	BucketFormat bucketFormat = BucketFormat.SPLUNK_BUCKET;
	when(bucket.getFormat()).thenReturn(bucketFormat);

	// Test
	URI archivePath = pathResolver.resolveArchivePath(bucket);

	// Verification
	String archivePathEnding = archiveRoot + "/" + clusterName + "/"
		+ serverName + "/" + bucketIndex + "/" + bucketFormat + "/"
		+ bucketName;
	assertTrue(archivePath.getPath().endsWith(archivePathEnding));
    }

    public void resolveArchivePath_givenWritableFileSystemUri_uriStartsWithFileSchemeAndWritablePath() {
	// Setup
	when(writableFileSystem.getWritableUri()).thenReturn(
		URI.create("hdfs://somehost:someport"));
	// Test
	URI archivePath = pathResolver.resolveArchivePath(bucket);

	// Verify
	assertEquals(writableFileSystem.getWritableUri()
		.getScheme(), archivePath.getScheme());
    }
}
