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

package com.splunk.shuttl.archiver.filesystem;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;

import org.apache.commons.io.FilenameUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsConf;
import com.splunk.shuttl.testutil.TUtilsMBean;

@Test(groups = { "fast-unit" })
public class PathResolverTest {

	/**
	 * 
	 */
	private ArchiveConfiguration configuration;
	private PathResolver pathResolver;
	private Bucket bucket;
	private String archivePath;
	private String clusterName;
	private String serverName;
	private String metadataDirName = "archive_meta";
	private String bucketIndex;
	private BucketFormat bucketFormat;
	private String bucketName;
	private String tmpDirectory;

	private final String ROOT_PATH = "/archive/path";

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		configuration = mock(ArchiveConfiguration.class);
		pathResolver = new PathResolver(configuration);
		stubArchiveConfiguration();
		bucketIndex = "index";
		bucketName = "bucket_name_id";
		bucketFormat = BucketFormat.SPLUNK_BUCKET;
		bucket = TUtilsBucket.createBucketWithIndexAndName(bucketIndex, bucketName);
	}

	private void stubArchiveConfiguration() {
		archivePath = ROOT_PATH;
		when(configuration.getArchiveDataPath()).thenReturn(archivePath);
		clusterName = "cluster_name";
		when(configuration.getClusterName()).thenReturn(clusterName);
		serverName = "server_name";
		when(configuration.getServerName()).thenReturn(serverName);
		tmpDirectory = "tmp_dir";
		when(configuration.getArchiveTempPath()).thenReturn(
				ROOT_PATH + "/" + tmpDirectory);
	}

	@Test(groups = { "fast-unit" })
	public void resolveArchivePath_givenValidBucket_combineBucketAndConfigurationToCreateTheEndingArchivePath() {
		String expected = getArchivePathUpToFormat();
		String actual = pathResolver.resolveArchivePath(bucket).toString();
		assertEquals(expected, actual);
	}

	public void resolveArchivePath_givenWritableFileSystemPath_pathStartsWithWritablePath() {
		// Test
		String archivePath = pathResolver.resolveArchivePath(bucket);

		// Verify
		assertTrue(archivePath.startsWith(configuration.getArchiveDataPath()));
	}

	public void getIndexesHome_givenNothing_returnsPathThatEndsWithThePathToWhereIndexesLive() {
		assertEquals(pathResolver.getIndexesHome().toString(),
				archiveServerCluster());
	}

	public void getIndexesHome_givenNothing_returnsPathThatStartsWithWritablePath() {
		assertTrue(pathResolver.getIndexesHome().startsWith(
				configuration.getArchiveDataPath()));
	}

	public void getBucketsHome_givenIndex_pathThatEndsWithWhereBucketsLive() {
		String expected = archiveServerCluster() + "/" + bucketIndex;
		String actual = pathResolver.getBucketsHome(bucketIndex).toString();
		assertEquals(expected, actual);
	}

	public void getBucketsHome_givenNothing_startsWithWritablePath() {
		assertTrue(pathResolver.getBucketsHome(null).toString()
				.startsWith(archivePath.toString()));
	}

	public void resolveIndexFromPathToBucket_givenValidPathToBucket_indexForTheBucket() {
		assertEquals(bucketIndex,
				pathResolver.resolveIndexFromPathToBucket(getArchivePathUpToBucket()));
	}

	public void resolveIndexFromPathToBucket_pathEndsWithSeparator_indexForBucket() {
		assertEquals(
				bucketIndex,
				pathResolver.resolveIndexFromPathToBucket(getArchivePathUpToBucket()
						+ "/"));
	}

	public void getFormatsHome_givenIndexAndBucketName_pathEqualsBucketsHomePlusBucketName() {
		String index = "index";
		String bucketName = "bucketName";
		String expectedFormatsHome = pathResolver.getBucketsHome(index).toString()
				+ "/" + bucketName;
		String actualFormatsHome = pathResolver.getFormatsHome(index, bucketName);
		assertEquals(expectedFormatsHome, actualFormatsHome);
	}

	public void resolveArchivedBucketPath_givenIndexBucketNameAndFormat_pathEqualsFormatsHomePlusFormat() {
		String index = "index";
		String bucketName = "bucketName";
		BucketFormat format = BucketFormat.UNKNOWN;
		String expectedBucketPath = pathResolver.getFormatsHome(index, bucketName)
				+ "/" + format;
		String actualBucketPath = pathResolver.resolveArchivedBucketPath(index,
				bucketName, format);
		assertEquals(expectedBucketPath, actualBucketPath);
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
		return archivePath.toString() + "/" + clusterName + "/" + serverName;
	}

	private String getArchivePathUpToBucketMetadata() {
		return getArchivePathUpToFormat() + "/" + metadataDirName;
	}

	public void resolveTempPathForBucket_givenBucket_tmpDirConcatWithBucketPath() {
		Bucket bucket = TUtilsBucket.createBucket();
		String path = pathResolver.resolveTempPathForBucket(bucket);
		String expected = configuration.getArchiveTempPath().toString()
				+ pathResolver.resolveArchivePath(bucket);
		assertEquals(expected, path);
	}

	public void getConfigured_registeredMBean_getsPathResolverInstance() {
		File shuttlConfsDir = TUtilsConf.getNullConfsDir();
		TUtilsMBean.runWithRegisteredMBeans(shuttlConfsDir, new Runnable() {
			@Override
			public void run() {
				PathResolver pr = PathResolver.getConfigured();
				assertNotNull(pr);
			}
		});
	}

	public void resolvePathForBucketMetadata_bucketAndFile_livesInMetadataFolderInTheBucket() {
		String wholeMetadataPath = pathResolver.resolvePathForBucketMetadata(
				bucket, createFile());

		// Using java.util.File because they I'm comparing paths, and File can
		// handle trailing separators.
		String expected = new File(getArchivePathUpToBucketMetadata())
				.getAbsolutePath();
		String actual = new File(wholeMetadataPath).getParentFile()
				.getAbsolutePath();
		assertEquals(expected, actual);
	}

	public void resolvePathForBucketMetadata_bucketAndFile_hasFileNameOfMetadataFile() {
		File metadataFile = createFile("metadata.file");
		String metadataPath = pathResolver.resolvePathForBucketMetadata(bucket,
				metadataFile);
		assertEquals(metadataFile.getName(), FilenameUtils.getName(metadataPath));
	}

	public void resolveTempPathForBucketSize_bucket_livesInTempMetadataDirectoryOfTempBucketPath() {
		String tempMetadataPath = pathResolver.resolveTempPathForBucketMetadata(
				bucket, createFile());
		String actualMetadataParentPath = new File(tempMetadataPath)
				.getParentFile().getAbsolutePath();
		assertEquals(pathResolver.resolveTempPathForBucket(bucket) + "/"
				+ metadataDirName, actualMetadataParentPath);
	}

	public void resolveTempPathForBucketMetadata_bucketAndFile_hasFileNameOfMetadataFile() {
		File metadataFile = createFile("fileName.meta");
		String tempPath = pathResolver.resolveTempPathForBucketMetadata(bucket,
				metadataFile);
		assertEquals(metadataFile.getName(), FilenameUtils.getName(tempPath));
	}

	public void getServerNamesHome_givenSetup_parentToIndexesPath() {
		String indexesHome = pathResolver.getIndexesHome();
		String indexesHomeParent = new File(indexesHome).getParentFile()
				.getAbsolutePath();

		String serversHome = pathResolver.getServerNamesHome();
		assertEquals(indexesHomeParent, serversHome);
	}

}
