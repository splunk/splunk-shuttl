package com.splunk.shep.archiver.archive;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.net.URI;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.fileSystem.WritableFileSystem;
import com.splunk.shep.archiver.model.Bucket;

@Test(groups = { "fast-unit" })
public class PathResolverTest {

    private ArchiveConfiguration configuration;
    private PathResolver pathResolver;
    private Bucket bucket;
    private WritableFileSystem writableFileSystem;
    private String archiveRoot;
    private String clusterName;
    private String serverName;
    private String writableUri;

    @BeforeMethod(groups = { "fast-unit" })
    public void setUp() {
	configuration = mock(ArchiveConfiguration.class);
	writableFileSystem = mock(WritableFileSystem.class);
	pathResolver = new PathResolver(configuration, writableFileSystem);
	stubArchiveConfiguration();
	stubWritableFileSystem();
	bucket = mock(Bucket.class);
    }

    private void stubWritableFileSystem() {
	writableUri = "hdfs://somehost:someport";
	when(writableFileSystem.getWritableUri()).thenReturn(
		URI.create(writableUri));
    }

    private void stubArchiveConfiguration() {
	archiveRoot = "archiving_root";
	when(configuration.getArchivingRoot()).thenReturn(archiveRoot);
	clusterName = "cluster_name";
	when(configuration.getClusterName()).thenReturn(clusterName);
	serverName = "server_name";
	when(configuration.getServerName()).thenReturn(serverName);
    }

    public void resolveArchivePath_givenValidBucket_combineBucketAndConfigurationToCreateTheEndingArchivePath() {
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

    public void resolveArchivePath_givenWritableFileSystemUri_uriStartsWithWritablePath() {
	// Test
	URI archivePath = pathResolver.resolveArchivePath(bucket);

	// Verify
	assertTrue(archivePath.toString().startsWith(
		writableFileSystem.getWritableUri().toString()));
    }

    public void getPathToIndexes_givenNothing_returnsPathThatEndsWithThePathToWhereIndexesLive() {
	URI indexesHome = pathResolver.getIndexesHome();

	String indexesHomeEnding = archiveRoot + "/" + clusterName + "/"
		+ serverName;
	assertTrue(indexesHome.getPath().endsWith(indexesHomeEnding));
    }

    public void getPathToIndexes_givenNothing_returnsPathThatStartsWithWritablePath() {
	URI indexesHome = pathResolver.getIndexesHome();

	assertTrue(indexesHome.toString().startsWith(
		writableFileSystem.getWritableUri().toString()));
    }

}
