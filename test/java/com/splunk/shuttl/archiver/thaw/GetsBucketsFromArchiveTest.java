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
package com.splunk.shuttl.archiver.thaw;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class GetsBucketsFromArchiveTest {

	private ThawBucketTransferer thawBucketTransferer;
	private BucketImporter bucketImporter;
	private GetsBucketsFromArchive getsBucketsFromArchive;
	private BucketSizeResolver bucketSizeResolver;
	private Bucket bucket;

	@BeforeMethod
	public void setUp() {
		thawBucketTransferer = mock(ThawBucketTransferer.class);
		bucketImporter = mock(BucketImporter.class);
		bucketSizeResolver = mock(BucketSizeResolver.class);
		getsBucketsFromArchive = new GetsBucketsFromArchive(thawBucketTransferer,
				bucketImporter, bucketSizeResolver);
		bucket = mock(Bucket.class);
	}

	public void _givenSuccessfulTransfer_importBucketAndGetSizeFromBucket()
			throws Exception {
		Bucket dummy = TUtilsBucket.createBucket();
		Bucket bucketThawed = mock(Bucket.class);
		when(thawBucketTransferer.transferBucketToThaw(bucket)).thenReturn(
				bucketThawed);
		when(bucketImporter.restoreToSplunkBucketFormat(bucketThawed)).thenReturn(
				dummy);
		when(bucketSizeResolver.resolveBucketSize(bucketThawed)).thenReturn(dummy);

		getsBucketsFromArchive.getBucketFromArchive(bucket);
	}

	public void _givenSuccessfulImportAndSizeResolving_returnImportedBucketWithSize()
			throws Exception {
		Bucket importedBucket = TUtilsBucket.createBucket();
		Bucket sizedBucket = mock(Bucket.class);
		stub(sizedBucket.getSize()).toReturn(121L);
		when(bucketImporter.restoreToSplunkBucketFormat(any(Bucket.class)))
				.thenReturn(importedBucket);
		when(bucketSizeResolver.resolveBucketSize(any(Bucket.class))).thenReturn(
				sizedBucket);
		Bucket actualBucket = getsBucketsFromArchive.getBucketFromArchive(bucket);

		assertEquals(importedBucket.getDirectory(), actualBucket.getDirectory());
		assertEquals(importedBucket.getEarliest(), actualBucket.getEarliest());
		assertEquals(importedBucket.getLatest(), actualBucket.getLatest());
		assertEquals(importedBucket.getIndex(), actualBucket.getIndex());
		assertEquals(importedBucket.getFormat(), actualBucket.getFormat());
		assertEquals(importedBucket.getName(), actualBucket.getName());
		assertEquals(importedBucket.getURI(), actualBucket.getURI());
		assertEquals(sizedBucket.getSize(), actualBucket.getSize());
	}

	// Sad path

	@Test(expectedExceptions = { ThawTransferFailException.class })
	public void _whenTransferBucketsFailToThaw_throwsExceptionAndDoesNotRestoreFailedBucket()
			throws Exception {
		Bucket bucket = mock(Bucket.class);
		doThrow(new IOException()).when(thawBucketTransferer).transferBucketToThaw(
				any(Bucket.class));
		getsBucketsFromArchive.getBucketFromArchive(bucket);
		verifyZeroInteractions(bucketImporter);
	}

}
