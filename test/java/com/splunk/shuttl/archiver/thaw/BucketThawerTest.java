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

import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.listers.ArchiveBucketsLister;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketThawerTest {

	BucketThawer bucketThawer;

	BucketFilter bucketFilter;
	ArchiveBucketsLister archiveBucketsLister;
	BucketFormatResolver bucketFormatResolver;
	ThawBucketTransferer thawBucketTransferer;
	BucketImporter bucketImporter;

	String index;
	Date latestTime;
	List<Bucket> buckets;
	Date earliestTime;

	@BeforeMethod
	public void setUp() {
		buckets = Arrays.asList(mock(Bucket.class));
		earliestTime = mock(Date.class);
		latestTime = mock(Date.class);

		archiveBucketsLister = mock(ArchiveBucketsLister.class);
		bucketFilter = mock(BucketFilter.class);
		bucketFormatResolver = mock(BucketFormatResolver.class);
		thawBucketTransferer = mock(ThawBucketTransferer.class);
		bucketImporter = mock(BucketImporter.class);
		bucketThawer = new BucketThawer(archiveBucketsLister, bucketFilter,
				bucketFormatResolver, thawBucketTransferer, bucketImporter);
		index = "index";
	}

	@Test(groups = { "fast-unit" })
	public void thawBuckets_givenTimeRange_filterBucketTimeRange() {
		when(archiveBucketsLister.listBucketsInIndex(anyString())).thenReturn(
				buckets);
		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		verify(bucketFilter).filterBucketsByTimeRange(buckets, earliestTime,
				latestTime);
	}

	public void thawBuckets_givenFilteredBuckets_resolveBucketsFormats() {
		List<Bucket> filteredBuckets = buckets;
		when(
				bucketFilter.filterBucketsByTimeRange(anyListOf(Bucket.class),
						any(Date.class), any(Date.class))).thenReturn(filteredBuckets);
		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		verify(bucketFormatResolver).resolveBucketsFormats(filteredBuckets);
	}

	public void thawBuckets_givenOneFilteredBucketWithFormat_transferBucketsToThawDirectory()
			throws IOException {
		Bucket bucketToThaw = mock(Bucket.class);
		stubFilteredBucketsWithFormat(bucketToThaw);
		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		verify(thawBucketTransferer).transferBucketToThaw(bucketToThaw);
	}

	public void thawBuckets_givenZeroFilteredBucketWithFormat_noInteractionsWithBucketTransferer() {
		stubFilteredBucketsWithFormat(new Bucket[0]);
		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		verifyZeroInteractions(thawBucketTransferer);
	}

	public void thawBuckets_givenTwoFilteredBucketWithFormat_transfersBothBuckets()
			throws IOException {
		Bucket bucket1 = mock(Bucket.class);
		Bucket bucket2 = mock(Bucket.class);
		stubFilteredBucketsWithFormat(bucket1, bucket2);
		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		verify(thawBucketTransferer).transferBucketToThaw(bucket1);
		verify(thawBucketTransferer).transferBucketToThaw(bucket2);
	}

	public void thawBuckets_givenOneFilteredBucketWithFormat_restoresBucketToSplunkBucketFormat() {
		Bucket bucket = TUtilsBucket.createBucket();
		stubFilteredBucketsWithFormat(bucket);
		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		verify(bucketImporter).restoreToSplunkBucketFormat(bucket);
	}

	public void thawBuckets_whenTransferBucketsFailToThaw_doesNotRestoreFailedBucket()
			throws IOException {
		Bucket bucket = TUtilsBucket.createBucket();
		stubFilteredBucketsWithFormat(bucket);

		doThrow(new IOException()).when(thawBucketTransferer).transferBucketToThaw(
				any(Bucket.class));
		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		verifyZeroInteractions(bucketImporter);
	}

	private void stubFilteredBucketsWithFormat(Bucket... bucket) {
		List<Bucket> filteredBucketsWithFormat = asList(bucket);
		when(bucketFormatResolver.resolveBucketsFormats(anyListOf(Bucket.class)))
				.thenReturn(filteredBucketsWithFormat);
	}
}
