package com.splunk.shep.archiver.archive;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.net.URI;

import org.junit.Ignore;
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
    private String archiveRoot;
    private String clusterName;
    private String serverName;
    private String writableUri;
    private String bucketIndex;
    private BucketFormat bucketFormat;
    private String bucketName;

    @BeforeMethod(groups = { "fast" })
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
	bucketIndex = "index";
	when(bucket.getIndex()).thenReturn(bucketIndex);
	bucketFormat = BucketFormat.SPLUNK_BUCKET;
	when(bucket.getFormat()).thenReturn(bucketFormat);
	bucketName = "bucket_name_id";
	when(bucket.getName()).thenReturn(bucketName);

	// Test
	URI archivePath = pathResolver.resolveArchivePath(bucket);

	// Verification
	String archivePathEnding = getArchivePathUpToFormat();
	assertTrue(archivePath.getPath().endsWith(archivePathEnding));
    }

    private String getArchivePathUpToBucket() {
	return getArchivePathUpToIndex() + "/" + bucketName;
    }

    private String getArchivePathUpToFormat() {
	return getArchivePathUpToBucket() + "/" + bucketFormat;
    }

    private String getArchivePathUpToIndex() {
	return archiveServerCluster() + "/" + bucketIndex;
    }

    private String archiveServerCluster() {
	return "/" + archiveRoot + "/" + clusterName + "/" + serverName;
    }

    public void resolveArchivePath_givenWritableFileSystemUri_uriStartsWithWritablePath() {
	// Test
	URI archivePath = pathResolver.resolveArchivePath(bucket);

	// Verify
	assertTrue(archivePath.toString().startsWith(
		writableFileSystem.getWritableUri().toString()));
    }

    public void getIndexesHome_givenNothing_returnsPathThatEndsWithThePathToWhereIndexesLive() {
	URI indexesHome = pathResolver.getIndexesHome();

	String indexesHomeEnding = archiveServerCluster();
	assertTrue(indexesHome.getPath().endsWith(indexesHomeEnding));
    }

    public void getIndexesHome_givenNothing_returnsPathThatStartsWithWritablePath() {
	URI indexesHome = pathResolver.getIndexesHome();

	assertTrue(indexesHome.toString().startsWith(
		writableFileSystem.getWritableUri().toString()));
    }

    public void getBucketsHome_givenIndex_uriWithPathThatEndsWithWhereBucketsLive() {
	String index = "index";
	URI bucketsHome = pathResolver.getBucketsHome(index);
	String bucketsHomeEnding = archiveServerCluster() + "/" + index;
	assertTrue(bucketsHome.getPath().endsWith(bucketsHomeEnding));
    }

    public void getBucketsHome_givenNothing_startsWithWritablePath() {
	assertTrue(pathResolver.getBucketsHome(null).toString()
		.startsWith(writableUri));
    }

    @Ignore
    public void resolveIndexFromUriToBucket_givenValidUriToBucket_indexForTheBucket() {

    }
}
