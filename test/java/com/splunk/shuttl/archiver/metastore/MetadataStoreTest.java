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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionExecuter;
import com.splunk.shuttl.archiver.filesystem.transaction.file.PutFileTransaction;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class MetadataStoreTest {

	private MetadataStore metadataStore;
	private PathResolver pathResolver;
	private FlatFileStorage flatFileStorage;
	private ArchiveFileSystem archiveFileSystem;
	private TransactionExecuter transactionExecuter;
	private LocalFileSystemPaths localFileSystemPaths;
	private Bucket bucket;

	@BeforeMethod
	public void setUp() {
		pathResolver = mock(PathResolver.class);
		flatFileStorage = mock(FlatFileStorage.class);
		archiveFileSystem = mock(ArchiveFileSystem.class);
		transactionExecuter = mock(TransactionExecuter.class);
		localFileSystemPaths = mock(LocalFileSystemPaths.class);
		metadataStore = new MetadataStore(pathResolver, flatFileStorage,
				archiveFileSystem, transactionExecuter, localFileSystemPaths);

		bucket = TUtilsBucket.createBucket();
	}

	public void put_bucketFileNameAndData_transfersFlatFileWithTransactionally() {
		File flatFile = mock(File.class);
		String filename = "filename";
		String temp = "temp";
		when(flatFileStorage.getFlatFile(bucket, filename)).thenReturn(flatFile);
		when(pathResolver.resolveTempPathForBucketMetadata(bucket, flatFile))
				.thenReturn(temp);
		String flatFileRealPath = "";
		when(pathResolver.resolvePathForBucketMetadata(bucket, flatFile))
				.thenReturn(flatFileRealPath);

		metadataStore.put(bucket, filename, "data");

		verify(transactionExecuter).execute(
				eq(PutFileTransaction.create(archiveFileSystem,
						flatFile.getAbsolutePath(), temp, flatFileRealPath)));
	}
}
