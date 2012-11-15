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

import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.FileOverwriteException;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.filesystem.transaction.AbstractTransaction;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionException;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionExecuter;
import com.splunk.shuttl.archiver.filesystem.transaction.bucket.PutBucketTransaction;
import com.splunk.shuttl.archiver.metastore.ArchiveBucketSize;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class ArchiveBucketTransfererTest {

	private ArchiveFileSystem archive;
	private PathResolver pathResolver;
	private ArchiveBucketTransferer archiveBucketTransferer;
	private ArchiveBucketSize archiveBucketSize;
	private TransactionExecuter transactionExecuter;

	@BeforeMethod
	public void setUp() {
		archive = mock(ArchiveFileSystem.class);
		pathResolver = mock(PathResolver.class);
		archiveBucketSize = mock(ArchiveBucketSize.class);
		transactionExecuter = mock(TransactionExecuter.class);
		archiveBucketTransferer = new ArchiveBucketTransferer(archive,
				pathResolver, archiveBucketSize, transactionExecuter);
	}

	@Test(groups = { "fast-unit" })
	public void transferBucketToArchive_givenValidBucketAndPath_putBucketWithArchiveFileSystem() {
		Bucket bucket = TUtilsBucket.createBucket();
		String destination = "/some/path";
		String temp = "/temp/path";
		when(pathResolver.resolveArchivePath(bucket)).thenReturn(destination);
		when(pathResolver.resolveTempPathForBucket(bucket)).thenReturn(temp);
		archiveBucketTransferer.transferBucketToArchive(bucket);
		verify(transactionExecuter).execute(
				eq(PutBucketTransaction.create(archive, bucket, temp, destination)));
	}

	public void transferBucketToArchive_givenSuccessfulBucketTransfer_startBucketSizeTransaction() {
		Bucket bucket = mock(Bucket.class);
		archiveBucketTransferer.transferBucketToArchive(bucket);
		verify(archiveBucketSize).persistBucketSize(bucket);
	}

	public void transferBucketToArchive_whenBucketTransferIsUnsuccessful_dontPutBucketSizeInArchive()
			throws FileNotFoundException, FileOverwriteException, IOException {
		doThrow(Exception.class).when(transactionExecuter).execute(
				any(AbstractTransaction.class));
		try {
			archiveBucketTransferer.transferBucketToArchive(mock(Bucket.class));
			fail();
		} catch (Exception e) {
		}
		verifyZeroInteractions(archiveBucketSize);
	}

	@Test(expectedExceptions = { FailedToArchiveBucketException.class })
	public void _archiveFileSystemThrowsFileNotFoundException_throwFailedToArchiveBucketException()
			throws IOException {
		doThrow(TransactionException.class).when(transactionExecuter).execute(
				any(AbstractTransaction.class));
		archiveBucketTransferer.transferBucketToArchive(mock(Bucket.class));
	}

	public void isArchived_bucketInFormatIsNotInArchiveFileSystem_false()
			throws IOException {
		Bucket bucket = TUtilsBucket.createBucket();
		String bucketPath = "/bucket/path";
		when(
				pathResolver.resolveArchivedBucketPath(bucket.getIndex(),
						bucket.getName(), bucket.getFormat())).thenReturn(bucketPath);
		when(archive.listPath(bucketPath)).thenReturn(new ArrayList<String>());

		assertFalse(archiveBucketTransferer.isArchived(bucket, bucket.getFormat()));
	}

	public void isArchived_bucketInFormatExistsInTheArchiveFileSystem_true()
			throws IOException {
		Bucket bucket = TUtilsBucket.createBucket();
		String bucketPath = "/bucket/path";
		when(
				pathResolver.resolveArchivedBucketPath(bucket.getIndex(),
						bucket.getName(), bucket.getFormat())).thenReturn(bucketPath);
		when(archive.listPath(bucketPath)).thenReturn(asList("/valid/path"));
		assertTrue(archiveBucketTransferer.isArchived(bucket, bucket.getFormat()));
	}
}
