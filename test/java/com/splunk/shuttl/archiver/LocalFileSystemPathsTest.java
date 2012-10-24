// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.splunk.shuttl.archiver;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class LocalFileSystemPathsTest {

	private LocalFileSystemPaths localFileSystemPaths;
	private String testDirectoryPath;
	private Bucket bucket;

	@BeforeMethod
	public void setUp() {
		bucket = TUtilsBucket.createBucket();
		testDirectoryPath = createFilePath().getAbsolutePath();
		localFileSystemPaths = new LocalFileSystemPaths(testDirectoryPath);
		removeArchiverDirectory();
	}

	private void removeArchiverDirectory() {
		File directory = localFileSystemPaths.getArchiverDirectory();
		FileUtils.deleteQuietly(directory);
	}

	@AfterMethod
	public void tearDown() {
		removeArchiverDirectory();
	}

	@Test(groups = { "fast-unit" })
	public void getArchiverDirectory_givenTestDirectory_dirIsChildToTestDirectory() {
		assertDoesNotExist(testDirectoryPath);
		File dir = localFileSystemPaths.getArchiverDirectory();
		assertEquals(testDirectoryPath, dir.getParent());
	}

	private void assertDoesNotExist(String path) {
		assertFalse(new File(path).exists());
	}

	public void getSafeLocation__dirExistsInsideArchiverDirectory() {
		File safeDir = localFileSystemPaths.getSafeDirectory();
		assertExistsInParentArchiverDirectory(safeDir);
	}

	private void assertExistsInParentArchiverDirectory(File dir) {
		assertTrue(dir.exists());
		assertEquals(localFileSystemPaths.getArchiverDirectory(),
				dir.getParentFile());
	}

	public void getFailLocation_setUp_dirExistsInsideArchiverDirectory() {
		File failLocation = localFileSystemPaths.getFailDirectory();
		assertExistsInParentArchiverDirectory(failLocation);
	}

	public void getArchiveLocksDirectory_bucket_uniquePerBucket() {
		File locksDir = localFileSystemPaths.getArchiveLocksDirectory(bucket);
		assertBucketUniquePathInsideArchiverDirectory(locksDir);
	}

	private void assertBucketUniquePathInsideArchiverDirectory(File dir) {
		assertTrue(dir.exists());
		String separator = File.separator;
		String bucketUniquePathInArchiverDir = bucket.getIndex() + separator
				+ bucket.getName() + separator + bucket.getFormat();
		String path = dir.getAbsolutePath();
		assertTrue(path.endsWith(bucketUniquePathInArchiverDir));

		assertTrue(path.contains(localFileSystemPaths.getArchiverDirectory()
				.getAbsolutePath()));
	}

	public void getExportDirectory_bucket_uniquePerBucket() {
		assertBucketUniquePathInsideArchiverDirectory(localFileSystemPaths
				.getExportDirectory(bucket));
	}

	public void getThawLocksDirectory_bucket_uniquePerBucket() {
		assertBucketUniquePathInsideArchiverDirectory(localFileSystemPaths
				.getThawLocksDirectory(bucket));
	}

	public void getThawLocksDirectoryForAllBuckets__dirExistsInsideArchiverDirectory() {
		assertExistsInParentArchiverDirectory(localFileSystemPaths
				.getThawLocksDirectoryForAllBuckets());
	}

	public void getThawLocksDirectory_bucket_dirExistsInsideThawLocksDirectoryForAllBuckets() {
		File thawLocksDirectory = localFileSystemPaths
				.getThawLocksDirectory(bucket);
		File thawLocksDirectoryForAllBuckets = localFileSystemPaths
				.getThawLocksDirectoryForAllBuckets();

		assertTrue(thawLocksDirectory.getAbsolutePath().contains(
				thawLocksDirectoryForAllBuckets.getAbsolutePath()));
	}

	public void getThawTransfersDirectory_bucket_uniquePerBucket() {
		assertBucketUniquePathInsideArchiverDirectory(localFileSystemPaths
				.getThawTransfersDirectory(bucket));
	}

	public void getThawTransfersDirectory_bucket_dirExistsInsideThawTransfersDirectoryForAllBuckets() {
		File thawTransfersDir = localFileSystemPaths
				.getThawTransfersDirectory(bucket);
		File thawTransfersDirForAllBuckets = localFileSystemPaths
				.getThawTransfersDirectoryForAllBuckets();

		assertTrue(thawTransfersDir.getAbsolutePath().contains(
				thawTransfersDirForAllBuckets.getAbsolutePath()));
	}

	public void getArchiverDirectory_givenTildeWithoutRoot_resolvesTildeAsUserHome() {
		File archiverDirectory = new LocalFileSystemPaths("~/archiver_directory")
				.getArchiverDirectory();
		File expected = new File(FileUtils.getUserDirectoryPath(),
				"archiver_directory");
		assertEquals(expected.getAbsolutePath(), archiverDirectory.getParentFile()
				.getAbsolutePath());
	}

	public void getArchiverDirectory_givenPathWithFileSchemeAndTilde_resolvesTildeAsUserHome() {
		File archiverDirectory = new LocalFileSystemPaths("file:/~/archiver_dir")
				.getArchiverDirectory();
		File expected = new File(FileUtils.getUserDirectoryPath(), "archiver_dir");
		assertEquals(expected.getAbsolutePath(), archiverDirectory.getParentFile()
				.getAbsolutePath());
	}

	public void getMetadataDirectory_bucket_uniquePerBucket() {
		assertBucketUniquePathInsideArchiverDirectory(localFileSystemPaths
				.getMetadataDirectory(bucket));
	}

	public void getMetadataTransfersDirectory_bucket_uniquePerBucket() {
		assertBucketUniquePathInsideArchiverDirectory(localFileSystemPaths
				.getMetadataTransfersDirectory(bucket));
	}

	@Test(expectedExceptions = { ArchiverMBeanNotRegisteredException.class })
	public void create_withNoArchiverMBeanRegistration_throwsRuntimeException() {
		LocalFileSystemPaths.create();
	}
}
