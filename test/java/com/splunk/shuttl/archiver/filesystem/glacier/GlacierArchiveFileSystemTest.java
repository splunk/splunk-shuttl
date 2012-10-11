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
package com.splunk.shuttl.archiver.filesystem.glacier;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketDeleter;
import com.splunk.shuttl.archiver.filesystem.transaction.bucket.TransfersBuckets;
import com.splunk.shuttl.archiver.importexport.tgz.TgzFormatExporter;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.model.RemoteBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class GlacierArchiveFileSystemTest {

	private GlacierClient glacierClient;
	private TransfersBuckets glacierBucketTransferer;

	private String temp;
	private String dst;
	private TgzFormatExporter tgzFormatExporter;
	private Logger logger;
	private BucketDeleter bucketDeleter;

	@BeforeMethod
	public void setUp() {
		glacierClient = mock(GlacierClient.class);
		tgzFormatExporter = mock(TgzFormatExporter.class);
		logger = mock(Logger.class);
		bucketDeleter = mock(BucketDeleter.class);
		glacierBucketTransferer = new GlacierArchiveFileSystem(null, glacierClient,
				tgzFormatExporter, logger, bucketDeleter).getBucketTransferer();

		temp = "/path/temp";
		dst = "/path/dst";
	}

	public void putBucket_givenTgzBucket_uploadBucketToTheRealDestination()
			throws IOException {
		LocalBucket tgzBucket = TUtilsBucket.createTgzBucket();
		glacierBucketTransferer.put(tgzBucket, temp, dst);
		verify(glacierClient).upload(eq(getBucketFile(tgzBucket)), eq(dst));
	}

	private File getBucketFile(LocalBucket tgzBucket) {
		return tgzBucket.getDirectory().listFiles()[0];
	}

	public void putBucket_givenSplunkBucket_exportBucketToTgzAndLogThisEventThenUploadTgz()
			throws IOException {
		LocalBucket bucket = TUtilsBucket.createBucket();
		LocalBucket tgzBucket = TUtilsBucket.createTgzBucket();
		when(tgzFormatExporter.exportBucket(bucket)).thenReturn(tgzBucket);
		glacierBucketTransferer.put(bucket, temp, dst);
		verify(glacierClient).upload(eq(getBucketFile(tgzBucket)), eq(dst));
		verify(logger).warn(anyString());
	}

	public void putBucket_givenSplunkBucket_exportedBucketGetsUploadedThenDeleted()
			throws IOException {
		LocalBucket bucket = TUtilsBucket.createBucket();
		LocalBucket tgzBucket = TUtilsBucket.createTgzBucket();
		when(tgzFormatExporter.exportBucket(bucket)).thenReturn(tgzBucket);
		glacierBucketTransferer.put(bucket, temp, dst);
		InOrder inOrder = inOrder(glacierClient, bucketDeleter);
		inOrder.verify(glacierClient).upload(eq(getBucketFile(tgzBucket)), eq(dst));
		inOrder.verify(bucketDeleter).deleteBucket(tgzBucket);
		inOrder.verifyNoMoreInteractions();
	}

	public void getBucket__getsBucketWithBucketsRemoteUri() throws IOException {
		RemoteBucket remoteBucket = TUtilsBucket.createRemoteBucket();
		File temp = mock(File.class);
		File dst = mock(File.class);
		glacierBucketTransferer.get(remoteBucket, temp, dst);
		verify(glacierClient).downloadToDir(remoteBucket.getPath(), temp);
	}

	@Test(expectedExceptions = { GlacierArchivingException.class })
	public void putBucket_glacierClientThrows_wrapsExceptionInGlacierArchivingException()
			throws IOException {
		doThrow(RuntimeException.class).when(glacierClient).upload(any(File.class),
				anyString());
		glacierBucketTransferer.put(TUtilsBucket.createTgzBucket(), temp, dst);
	}

	@Test(expectedExceptions = { GlacierThawingException.class })
	public void getBucket_glacierClientThrows_wrapsExceptionInGlacierThawingException()
			throws IOException {
		doThrow(RuntimeException.class).when(glacierClient).downloadToDir(
				anyString(), any(File.class));
		glacierBucketTransferer.get(TUtilsBucket.createRemoteBucket(),
				mock(File.class), mock(File.class));
	}
}
