// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.archiver.archive;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.net.URI;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsMBean;

@Test(groups = { "fast-unit" })
public class PathResolverTest {

	private ArchiveConfiguration configuration;
	private PathResolver pathResolver;
	private Bucket bucket;
	private URI archiveRoot;
	private String clusterName;
	private String serverName;
	private String bucketIndex;
	private BucketFormat bucketFormat;
	private String bucketName;

	private final String ROOT_URI = "hdfs://somehost:12345";

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		configuration = mock(ArchiveConfiguration.class);
		pathResolver = new PathResolver(configuration);
		stubArchiveConfiguration();
		bucketIndex = "index";
		bucketName = "bucket_name_id";
		bucketFormat = BucketFormat.SPLUNK_BUCKET;
		bucket = TUtilsBucket.createBucketWithIndexAndName(bucketIndex,
				bucketName);
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
		String expected = getArchivePathUpToFormat();
		String actual = pathResolver.resolveArchivePath(bucket).toString();
		assertEquals(expected, actual);
	}

	public void resolveArchivePath_givenWritableFileSystemUri_uriStartsWithWritablePath() {
		// Test
		URI archivePath = pathResolver.resolveArchivePath(bucket);

		// Verify
		assertTrue(archivePath.toString().startsWith(
				configuration.getArchivingRoot().toString()));
	}

	public void getIndexesHome_givenNothing_returnsPathThatEndsWithThePathToWhereIndexesLive() {
		assertEquals(pathResolver.getIndexesHome().toString(),
				archiveServerCluster());
	}

	public void getIndexesHome_givenNothing_returnsPathThatStartsWithWritablePath() {
		assertTrue(pathResolver.getIndexesHome().toString()
				.startsWith(configuration.getArchivingRoot().toString()));
	}

	public void getBucketsHome_givenIndex_uriWithPathThatEndsWithWhereBucketsLive() {
		String expected = archiveServerCluster() + "/" + bucketIndex;
		String actual = pathResolver.getBucketsHome(bucketIndex).toString();
		assertEquals(expected, actual);
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
		return archiveRoot.toString() + "/" + clusterName + "/" + serverName;
	}

	public void getConfigured_registeredMBean_getsPathResolverInstance() {
		TUtilsMBean.registerShuttlArchiverMBean();
		PathResolver pr = PathResolver.getConfigured();
		assertNotNull(pr);
	}

}
