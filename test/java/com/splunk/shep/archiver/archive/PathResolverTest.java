package com.splunk.shep.archiver.archive;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.net.URI;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.fileSystem.WritableFileSystem;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast-unit" })
public class PathResolverTest {

    private ArchiveConfiguration configuration;
    private PathResolver pathResolver;
    private Bucket bucket;
    private WritableFileSystem writableFileSystem;
    private URI archiveRoot;
    private URI writableUri;
    private String clusterName;
    private String serverName;
    private String bucketIndex;
    private BucketFormat bucketFormat;
    private String bucketName;

    private final String ROOT_URI = "hdfs://somehost:someport";

    @BeforeMethod(groups = { "fast-unit" })
    public void setUp() {
	configuration = mock(ArchiveConfiguration.class);
	writableFileSystem = mock(WritableFileSystem.class);
	pathResolver = new PathResolver(configuration, writableFileSystem);
	stubArchiveConfiguration();
	stubWritableFileSystem();
	bucketIndex = "index";
	bucketName = "bucket_name_id";
	bucketFormat = BucketFormat.SPLUNK_BUCKET;
	bucket = UtilsBucket.createTestBucketWithIndexAndName(bucketIndex,
		bucketName);
    }

    private void stubWritableFileSystem() {
	writableUri = URI.create(ROOT_URI);
	when(writableFileSystem.getWritableUri()).thenReturn(writableUri);
     }

    private void stubArchiveConfiguration() {
	archiveRoot = URI.create(ROOT_URI);
	when(configuration.getArchivingRoot()).thenReturn(archiveRoot);
	clusterName = "cluster_name";
	when(configuration.getClusterName()).thenReturn(clusterName);
	serverName = "server_name";
	when(configuration.getServerName()).thenReturn(serverName);
    }

    @Test(groups = { "fast-unit" })
    public void resolveArchivePath_givenValidBucket_combineBucketAndConfigurationToCreateTheEndingArchivePath() {
	assertEquals(pathResolver.resolveArchivePath(bucket).toString(),
		getArchivePathUpToFormat());
    }

    public void resolveArchivePath_givenWritableFileSystemUri_uriStartsWithWritablePath() {
	// Test
	URI archivePath = pathResolver.resolveArchivePath(bucket);

	// Verify
	assertTrue(archivePath.toString().startsWith(
		writableFileSystem.getWritableUri().toString()));
    }

    public void getIndexesHome_givenNothing_returnsPathThatEndsWithThePathToWhereIndexesLive() {
	assertEquals(pathResolver.getIndexesHome().toString(),
		archiveServerCluster());
    }

    public void getIndexesHome_givenNothing_returnsPathThatStartsWithWritablePath() {
	assertTrue(pathResolver.getIndexesHome().toString()
		.startsWith(writableFileSystem.getWritableUri().toString()));
    }

    public void getBucketsHome_givenIndex_uriWithPathThatEndsWithWhereBucketsLive() {
	assertEquals(pathResolver.getBucketsHome(bucketIndex).toString(),
		archiveServerCluster() + "/" + bucketIndex);
    }

    public void getBucketsHome_givenNothing_startsWithWritablePath() {
	assertTrue(pathResolver.getBucketsHome(null).toString()
		.startsWith(archiveRoot.toString()));
    }

    public void resolveIndexFromUriToBucket_givenValidUriToBucket_indexForTheBucket() {
	String archivePathUpToBucket = getArchivePathUpToBucket();
	URI bucketURI = URI.create("schema:/" + archivePathUpToBucket);
	assertEquals(bucketIndex,
		pathResolver.resolveIndexFromUriToBucket(bucketURI));
    }

    public void resolveIndexFromUriToBucket_uriEndsWithSeparator_indexForBucket() {
	String archivePathUpToBucket = getArchivePathUpToBucket();
	URI bucketURI = URI.create("schema:/" + archivePathUpToBucket + "/");
	assertEquals(bucketIndex,
		pathResolver.resolveIndexFromUriToBucket(bucketURI));
    }

    public void getFormatsHome_givenIndexAndBucketName_uriEqualsBucketsHomePlusBucketName() {
	String index = "index";
	String bucketName = "bucketName";
	URI expectedFormatsHome = URI.create(pathResolver.getBucketsHome(index)
		.toString() + "/" + bucketName);
	URI actualFormatsHome = pathResolver.getFormatsHome(index, bucketName);
	assertEquals(expectedFormatsHome, actualFormatsHome);
    }

    public void resolveArchivedBucketURI_givenIndexBucketNameAndFormat_uriEqualsFormatsHomePlusFormat() {
	String index = "index";
	String bucketName = "bucketName";
	BucketFormat format = BucketFormat.UNKNOWN;
	URI expectedBucketUri = URI.create(pathResolver.getFormatsHome(index,
		bucketName) + "/" + format);
	URI actualBucketUri = pathResolver.resolveArchivedBucketURI(index,
		bucketName, format);
	assertEquals(expectedBucketUri, actualBucketUri);
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
	return archiveRoot + "/" + clusterName + "/" + serverName;
    }
}
