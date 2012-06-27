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

@Test(groups = { "fast-unit" })
public class GetsBucketsFromArchiveTest {

	private ThawBucketTransferer thawBucketTransferer;
	private BucketImporter bucketImporter;
	private GetsBucketsFromArchive getsBucketsFromArchive;

	@BeforeMethod
	public void setUp() {
		thawBucketTransferer = mock(ThawBucketTransferer.class);
		bucketImporter = mock(BucketImporter.class);
		getsBucketsFromArchive = new GetsBucketsFromArchive(thawBucketTransferer,
				bucketImporter);
	}

	public void _givenOneFilteredBucketWithFormat_transferBucketsToThawDirectory()
			throws Exception {
		Bucket bucketToThaw = mock(Bucket.class);
		getsBucketsFromArchive.getBucketFromArchive(bucketToThaw);
		verify(thawBucketTransferer).transferBucketToThaw(bucketToThaw);
	}

	public void _givenSuccessfulTransfer_importBucket() throws Exception {
		Bucket bucketToThaw = mock(Bucket.class);
		Bucket bucketThawed = mock(Bucket.class);
		when(thawBucketTransferer.transferBucketToThaw(bucketToThaw)).thenReturn(
				bucketThawed);
		getsBucketsFromArchive.getBucketFromArchive(bucketToThaw);
		verify(bucketImporter).restoreToSplunkBucketFormat(bucketThawed);
	}

	public void _givenSuccessfulImport_returnImportedBucket() throws Exception {
		Bucket importedBucket = mock(Bucket.class);
		when(bucketImporter.restoreToSplunkBucketFormat(any(Bucket.class)))
				.thenReturn(importedBucket);
		Bucket actualBucket = getsBucketsFromArchive
				.getBucketFromArchive(mock(Bucket.class));
		assertEquals(importedBucket, actualBucket);
	}

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
