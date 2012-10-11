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

import static org.mockito.Matchers.*;
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
	private BucketSizeIO bucketSizeIO;

	@BeforeMethod
	public void setUp() {
		pathResolver = mock(PathResolver.class);
		archiveFileSystem = mock(ArchiveFileSystem.class);
		bucketSizeIO = mock(BucketSizeIO.class);
		archiveBucketSize = new ArchiveBucketSize(pathResolver, bucketSizeIO,
				archiveFileSystem);
	}

	public void getBucketSizeTransaction_givenBucket_createsWithPathResolverPaths() {
		Bucket bucket = TUtilsBucket.createBucket();
		File src = mock(File.class);
		String temp = "/temp/path";
		String dst = "/dst/path";
		when(bucketSizeIO.getFileWithBucketSize(bucket)).thenReturn(src);
		when(pathResolver.resolveTempPathForBucketSize(bucket)).thenReturn(temp);
		when(pathResolver.getBucketSizeFilePathForBucket(bucket)).thenReturn(dst);

		Transaction bucketSizeTransaction = archiveBucketSize
				.getBucketSizeTransaction(bucket);
		assertEquals(PutFileTransaction.create(archiveFileSystem,
				src.getAbsolutePath(), temp, dst), bucketSizeTransaction);
	}

	public void getSize_givenPathToFileWithBucketSize_passesPathToBucketSizeFileForReading() {
		Bucket remoteBucket = TUtilsBucket.createRemoteBucket();
		String pathToFileWIthBucketSize = "path/to/bucket/size";
		when(pathResolver.getBucketSizeFilePathForBucket(remoteBucket)).thenReturn(
				pathToFileWIthBucketSize);
		archiveBucketSize.getSize(remoteBucket);
		verify(bucketSizeIO).readSizeFromRemoteFile(pathToFileWIthBucketSize);
	}

	public void getSize_givenBucketFileSizeReadSuccessfully_returnValue() {
		long size = 4711;
		when(bucketSizeIO.readSizeFromRemoteFile(anyString())).thenReturn(size);
		long actualSize = archiveBucketSize.getSize(TUtilsBucket
				.createRemoteBucket());
		assertEquals(size, actualSize);

	}
}
