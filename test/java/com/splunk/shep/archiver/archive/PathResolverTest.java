package com.splunk.shep.archiver.archive;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsTestNG;

@Test(groups = { "fast" })
public class PathResolverTest {

    public void resolveArchivePath_givenValidBucket_combineArchiveConfigurationAndBucketToReturnArchivePath() {
	ArchiveConfiguration config = mock(ArchiveConfiguration.class);
	Bucket bucket = mock(Bucket.class);

	String archiveRoot = "archiving_root";
	when(config.getArchivingRoot()).thenReturn(archiveRoot);
	String clusterName = "cluster_name";
	when(config.getClusterName()).thenReturn(clusterName);
	String serverName = "server_name";
	when(config.getServerName()).thenReturn(serverName);
	String bucketName = "bucket_name_id";
	when(bucket.getName()).thenReturn(bucketName);
	String bucketIndex = "index";
	when(bucket.getIndex()).thenReturn(bucketIndex);
	BucketFormat bucketFormat = BucketFormat.SPLUNK_BUCKET;
	when(bucket.getFormat()).thenReturn(bucketFormat);

	// Test
	PathResolver pathResolver = new PathResolver(config);
	URI archivePath = pathResolver.resolveArchivePath(bucket);

	// Verification
	URI expected = getURISafe("file:/" + archiveRoot + "/" + clusterName
		+ "/" + serverName + "/" + bucketIndex + "/" + bucketFormat
		+ "/" + bucketName);
	assertEquals(expected, archivePath);
    }

    private URI getURISafe(String uri) {
	try {
	    return new URI(uri);
	} catch (URISyntaxException e) {
	    e.printStackTrace();
	    UtilsTestNG.failForException("Could not create URI with String: "
		    + uri, e);
	    return null;
	}
    }

}
