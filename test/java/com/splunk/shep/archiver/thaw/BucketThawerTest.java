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
package com.splunk.shep.archiver.thaw;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.listers.ArchiveBucketsLister;
import com.splunk.shep.archiver.model.Bucket;

@Test(groups = { "fast-unit" })
public class BucketThawerTest {

    BucketThawer bucketThawer;

    BucketFilter bucketFilter;
    ArchiveBucketsLister archiveBucketsLister;
    BucketFormatResolver bucketFormatResolver;
    ThawBucketTransferer thawBucketTransferer;

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
	bucketThawer = new BucketThawer(archiveBucketsLister, bucketFilter,
		bucketFormatResolver, thawBucketTransferer);
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

    public void thawBuckes_givenFilteredBuckets_resolveBucketsFormats() {
	List<Bucket> filteredBuckets = buckets;
	when(
		bucketFilter.filterBucketsByTimeRange(anyListOf(Bucket.class),
			any(Date.class), any(Date.class))).thenReturn(
		filteredBuckets);
	bucketThawer.thawBuckets(index, earliestTime, latestTime);
	verify(bucketFormatResolver).resolveBucketsFormats(filteredBuckets);
    }

    public void thawBuckets_givenOneFilteredBucketWithFormatSet_transferBucketsToThawDirectory()
	    throws IOException {
	Bucket bucketToThaw = mock(Bucket.class);
	List<Bucket> filteredBucketsWithFormatSet = Arrays.asList(bucketToThaw);

	when(
		bucketFormatResolver
			.resolveBucketsFormats(anyListOf(Bucket.class)))
		.thenReturn(filteredBucketsWithFormatSet);
	bucketThawer.thawBuckets(index, earliestTime, latestTime);
	verify(thawBucketTransferer).transferBucketToThaw(bucketToThaw);
    }

    public void thawBuckets_givenZeroFilteredBucketWithFormatSet_noInteractionsWithBucketTransferer() {
	List<Bucket> emptyListOfBuckets = Arrays.asList();
	when(
		bucketFormatResolver
			.resolveBucketsFormats(anyListOf(Bucket.class)))
		.thenReturn(emptyListOfBuckets);
	bucketThawer.thawBuckets(index, earliestTime, latestTime);
	verifyZeroInteractions(thawBucketTransferer);
    }

    public void thawBuckets_givenTwoFilteredBucketWithFormatSet_noInteractionsWithBucketTransferer()
	    throws IOException {
	Bucket bucket1 = mock(Bucket.class);
	Bucket bucket2 = mock(Bucket.class);
	List<Bucket> emptyListOfBuckets = Arrays.asList(bucket1, bucket2);
	when(
		bucketFormatResolver
			.resolveBucketsFormats(anyListOf(Bucket.class)))
		.thenReturn(emptyListOfBuckets);
	bucketThawer.thawBuckets(index, earliestTime, latestTime);
	verify(thawBucketTransferer).transferBucketToThaw(bucket1);
	verify(thawBucketTransferer).transferBucketToThaw(bucket2);
    }

}
