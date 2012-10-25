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
package com.splunk.shuttl.archiver.metastore;

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
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionExecuter;
import com.splunk.shuttl.archiver.metastore.FlatFileStorage.FlatFileReadException;
import com.splunk.shuttl.archiver.metastore.MetadataStore.CouldNotReadMetadataException;
import com.splunk.shuttl.archiver.model.RemoteBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFunctional;

@Test(groups = { "slow-unit" })
public class MetadataStoreReadIntegrationTest {

	private ArchiveFileSystem localFileSystem;
	private PathResolver pathResolver;
	private RemoteBucket remoteBucket;
	private FlatFileStorage flatFileStorage;
	private File remoteMetadata;
	private LocalFileSystemPaths localFileSystemPaths;
	private File localMetadata;
	private Long expectedSize;
	private MetadataStore metadataStore;
	private String fileName;

	@BeforeMethod
	public void setUp() {
		ArchiveConfiguration config = TUtilsFunctional
				.getLocalFileSystemConfiguration();
		localFileSystem = ArchiveFileSystemFactory.getWithConfiguration(config);

		pathResolver = new PathResolver(config);
		localFileSystemPaths = new LocalFileSystemPaths(createDirectory());
		flatFileStorage = new FlatFileStorage(localFileSystemPaths);
		metadataStore = new MetadataStore(pathResolver, flatFileStorage,
				localFileSystem, new TransactionExecuter(), localFileSystemPaths);

		remoteBucket = TUtilsBucket.createRemoteBucket();
		expectedSize = 123L;

		fileName = "FileNameOfMetadataFile.file";
		localMetadata = flatFileStorage.getFlatFile(remoteBucket, fileName);
		String metadataPath = pathResolver.resolvePathForBucketMetadata(
				remoteBucket, localMetadata);
		remoteMetadata = new File(metadataPath);

		FileUtils.deleteQuietly(remoteMetadata);
		FileUtils.deleteQuietly(localMetadata);
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(remoteMetadata);
		FileUtils.deleteQuietly(localMetadata);
	}

	@Test(expectedExceptions = { CouldNotReadMetadataException.class })
	public void readBucketSize_sizeMetadataDoesNotExistLocallyNorRemotely_throws() {
		assertFalse(remoteMetadata.exists());
		assertFalse(localMetadata.exists());
		metadataStore.read(remoteBucket, fileName);
	}

	public void readBucketSize_sizeMetadataExistsRemotely_readsSize() {
		flatFileStorage.writeFlatFile(remoteMetadata, expectedSize);
		assertTrue(remoteMetadata.exists());
		assertFalse(localMetadata.exists());

		assertEquals(expectedSize, metadataStore.read(remoteBucket, fileName));
	}

	public void readBucketSize_sizeMetadataExistsLocally_readsSize()
			throws IOException {
		flatFileStorage.writeFlatFile(localMetadata, expectedSize);
		assertTrue(localMetadata.exists());
		assertNotNull(flatFileStorage.readFlatFile(localMetadata));

		assertEquals(expectedSize, metadataStore.read(remoteBucket, fileName));
	}

	public void readBucketSize_localExistingMetadataDoesNotContainSize_getsSize()
			throws IOException {
		flatFileStorage.writeFlatFile(remoteMetadata, expectedSize);
		assertTrue(remoteMetadata.exists());
		assertTrue(localMetadata.createNewFile());
		assertTrue(localMetadata.exists());
		try {
			flatFileStorage.readFlatFile(localMetadata);
			fail();
		} catch (FlatFileReadException e) {
		}

		assertEquals(expectedSize, metadataStore.read(remoteBucket, fileName));
	}

	@Test(expectedExceptions = { CouldNotReadMetadataException.class })
	public void readBucketSize_localMetaDoesNotContainDataAndRemoteDoesNotExist_throws()
			throws IOException {
		assertTrue(localMetadata.createNewFile());
		assertFalse(remoteMetadata.exists());
		assertTrue(localMetadata.exists());
		metadataStore.read(remoteBucket, localMetadata.getName());
	}
}
