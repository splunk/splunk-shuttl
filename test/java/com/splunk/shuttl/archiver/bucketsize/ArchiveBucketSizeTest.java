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

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.transaction.Transaction;
import com.splunk.shuttl.archiver.filesystem.transaction.file.PutFileTransaction;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class ArchiveBucketSizeTest {

	private ArchiveBucketSize archiveBucketSize;
	private PathResolver pathResolver;
	private ArchiveFileSystem archiveFileSystem;
	private FlatFileStorage flatFileStorage;

	@BeforeMethod
	public void setUp() {
		pathResolver = mock(PathResolver.class);
		archiveFileSystem = mock(ArchiveFileSystem.class);
		flatFileStorage = mock(FlatFileStorage.class);
		archiveBucketSize = new ArchiveBucketSize(pathResolver, archiveFileSystem,
				flatFileStorage, null);
	}

	public void getBucketSizeTransaction_givenBucket_createsWithPathResolverPaths() {
		Bucket bucket = TUtilsBucket.createBucket();
		File src = mock(File.class);
		String temp = "/temp/path";
		String dst = "/dst/path";
		String fileName = ArchiveBucketSize.FILE_NAME;
		when(flatFileStorage.getFlatFile(bucket, fileName)).thenReturn(src);
		when(pathResolver.resolveTempPathForBucketMetadata(bucket, src))
				.thenReturn(temp);
		when(pathResolver.resolvePathForBucketMetadata(bucket, src))
				.thenReturn(dst);

		Transaction bucketSizeTransaction = archiveBucketSize
				.getBucketSizeTransaction(bucket);
		assertEquals(PutFileTransaction.create(archiveFileSystem,
				src.getAbsolutePath(), temp, dst), bucketSizeTransaction);
	}
}
