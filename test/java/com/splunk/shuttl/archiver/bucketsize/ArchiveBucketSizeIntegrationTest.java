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
package com.splunk.shuttl.archiver.bucketsize;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.model.RemoteBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFunctional;

@Test(groups = { "slow-unit" })
public class ArchiveBucketSizeIntegrationTest {

	private ArchiveFileSystem localFileSystem;
	private PathResolver pathResolver;
	private ArchiveBucketSize archiveBucketSize;
	private RemoteBucket remoteBucket;
	private FlatFileStorage flatFileStorage;
	private File remoteMetadata;
	private LocalFileSystemPaths localFileSystemPaths;
	private File localMetadata;
	private Long expectedSize;

	@BeforeMethod
	public void setUp() {
		ArchiveConfiguration config = TUtilsFunctional
				.getLocalFileSystemConfiguration();
		localFileSystem = ArchiveFileSystemFactory.getWithConfiguration(config);

		pathResolver = new PathResolver(config);
		localFileSystemPaths = new LocalFileSystemPaths(createDirectory());
		flatFileStorage = new FlatFileStorage(localFileSystemPaths);
		archiveBucketSize = new ArchiveBucketSize(pathResolver, localFileSystem,
				flatFileStorage, localFileSystemPaths);

		remoteBucket = TUtilsBucket.createRemoteBucket();
		expectedSize = 123L;

		String metadataPath = pathResolver.resolvePathForBucketMetadata(
				remoteBucket,
				flatFileStorage.getFlatFile(remoteBucket, ArchiveBucketSize.FILE_NAME));
		remoteMetadata = new File(metadataPath);

		File metadataDirectory = localFileSystemPaths
				.getMetadataDirectory(remoteBucket);
		localMetadata = new File(metadataDirectory, ArchiveBucketSize.FILE_NAME);

		FileUtils.deleteQuietly(remoteMetadata);
		FileUtils.deleteQuietly(localMetadata);
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(remoteMetadata);
		FileUtils.deleteQuietly(localMetadata);
	}

	public void getSize_sizeMetadataDoesNotExistLocallyNorRemotely_null() {
		assertFalse(remoteMetadata.exists());
		assertFalse(localMetadata.exists());
		Long size = archiveBucketSize.getSize(remoteBucket);
		assertNull(size);
	}

	public void getSize_sizeMetadataExistsRemotely_readsSize() {
		flatFileStorage.writeFlatFile(remoteMetadata, expectedSize);
		assertTrue(remoteMetadata.exists());
		assertFalse(localMetadata.exists());

		Long size = archiveBucketSize.getSize(remoteBucket);
		assertEquals(expectedSize, size);
	}

	public void getSize_sizeMetadataExistsLocally_readsSize() throws IOException {
		flatFileStorage.writeFlatFile(localMetadata, expectedSize);
		assertTrue(localMetadata.exists());
		assertNotNull(flatFileStorage.readFlatFile(FileUtils
				.openInputStream(localMetadata)));

		Long size = archiveBucketSize.getSize(remoteBucket);
		assertEquals(expectedSize, size);
	}

	public void getSize_localExistingMetadataDoesNotContainSize_getsSize()
			throws IOException {
		flatFileStorage.writeFlatFile(remoteMetadata, expectedSize);
		assertTrue(localMetadata.createNewFile());
		assertTrue(remoteMetadata.exists());
		assertTrue(localMetadata.exists());
		assertNull(flatFileStorage.readFlatFile(FileUtils
				.openInputStream(localMetadata)));

		Long size = archiveBucketSize.getSize(remoteBucket);
		assertEquals(expectedSize, size);
	}

	public void getSize_localMetaDoesNotContainDataAndRemoteDoesNotExist_null()
			throws IOException {
		assertTrue(localMetadata.createNewFile());
		assertFalse(remoteMetadata.exists());
		assertTrue(localMetadata.exists());
		assertNull(flatFileStorage.readFlatFile(FileUtils
				.openInputStream(localMetadata)));

		assertNull(archiveBucketSize.getSize(remoteBucket));
	}
}
