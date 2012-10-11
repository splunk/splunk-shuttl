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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.bucketlock.BucketLock;
import com.splunk.shuttl.archiver.bucketlock.BucketLocker;
import com.splunk.shuttl.archiver.bucketlock.BucketLockerInTestDir;
import com.splunk.shuttl.archiver.listers.ListsBucketsFiltered;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.thaw.BucketThawer.FailedBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketThawerTest {

	private BucketThawer bucketThawer;
	private ListsBucketsFiltered listsBucketsFiltered;
	private GetsBucketsFromArchive getsBucketsFromArchive;
	private String index;
	private Date earliestTime;
	private Date latestTime;
	private Bucket bucket;
	private ThawLocationProvider thawLocationProvider;
	private BucketLocker thawBucketLocker;

	@BeforeMethod
	public void setUp() {
		listsBucketsFiltered = mock(ListsBucketsFiltered.class);
		getsBucketsFromArchive = mock(GetsBucketsFromArchive.class);
		thawLocationProvider = mock(ThawLocationProvider.class);
		thawBucketLocker = new BucketLockerInTestDir(createDirectory());
		bucketThawer = new BucketThawer(listsBucketsFiltered,
				getsBucketsFromArchive, thawLocationProvider, thawBucketLocker);

		index = "foo";
		earliestTime = new Date();
		latestTime = new Date(earliestTime.getTime() + 100);
		bucket = mock(Bucket.class);
	}

	public void thawBuckets_givenZeroBucketsWithinTimeRange_getsNoBuckets() {
		when(
				listsBucketsFiltered.listFilteredBucketsAtIndex(index, earliestTime,
						latestTime)).thenReturn(new ArrayList<Bucket>());
		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		verifyZeroInteractions(getsBucketsFromArchive);
	}

	public void thawBuckets_givenTwoBucketsWithinTimeRange_getsBuckets()
			throws ThawTransferFailException, ImportThawedBucketFailException {
		Bucket archivedBucketWithinTimeRange1 = mock(Bucket.class);
		Bucket archivedBucketWithinTimeRange2 = mock(Bucket.class);
		when(
				listsBucketsFiltered.listFilteredBucketsAtIndex(index, earliestTime,
						latestTime)).thenReturn(
				asList(archivedBucketWithinTimeRange1, archivedBucketWithinTimeRange2));

		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		verify(getsBucketsFromArchive).getBucketFromArchive(
				archivedBucketWithinTimeRange1);
		verify(getsBucketsFromArchive).getBucketFromArchive(
				archivedBucketWithinTimeRange2);
	}

	public void thawBuckets_bucketAlreadyThawedToThawLocation_doesNotThawBucketAgain()
			throws IOException {
		LocalBucket thawedBucket = TUtilsBucket.createBucket();
		when(thawLocationProvider.getLocationInThawForBucket(thawedBucket))
				.thenReturn(thawedBucket.getDirectory());
		when(
				listsBucketsFiltered.listFilteredBucketsAtIndex(index, earliestTime,
						latestTime)).thenReturn(asList((Bucket) thawedBucket));

		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		verifyZeroInteractions(getsBucketsFromArchive);
		assertEquals(thawedBucket, bucketThawer.getSkippedBuckets().get(0));
	}

	public void thawBuckets_thawLocationProviderThrowsException_failBucketAndDoNotTransfer()
			throws IOException {
		doThrow(new IOException()).when(thawLocationProvider)
				.getLocationInThawForBucket(bucket);
		when(
				listsBucketsFiltered.listFilteredBucketsAtIndex(index, earliestTime,
						latestTime)).thenReturn(asList(bucket));

		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		assertEquals(bucket, bucketThawer.getFailedBuckets().get(0).bucket);
		verifyZeroInteractions(getsBucketsFromArchive);
	}

	public void thawBuckets_bucketIsAlreadyLocked_doesNotThaw() {
		BucketLock bucketLock = thawBucketLocker.getLockForBucket(bucket);
		assertTrue(bucketLock.tryLockExclusive());
		when(
				listsBucketsFiltered.listFilteredBucketsAtIndex(index, earliestTime,
						latestTime)).thenReturn(asList(bucket));

		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		verifyZeroInteractions(getsBucketsFromArchive);
		assertEquals(bucket, bucketThawer.getSkippedBuckets().get(0));
	}

	public void getThawedBuckets_gotBucketFromArchive_returnBucket()
			throws ThawTransferFailException, ImportThawedBucketFailException {
		Bucket bucket1 = mock(Bucket.class);
		Bucket bucket2 = mock(Bucket.class);
		when(
				listsBucketsFiltered.listFilteredBucketsAtIndex(index, earliestTime,
						latestTime)).thenReturn(asList(bucket1, bucket2));
		LocalBucket thawedBucket1 = mock(LocalBucket.class);
		LocalBucket thawedBucket2 = mock(LocalBucket.class);
		when(getsBucketsFromArchive.getBucketFromArchive(bucket1)).thenReturn(
				thawedBucket1);
		when(getsBucketsFromArchive.getBucketFromArchive(bucket2)).thenReturn(
				thawedBucket2);
		bucketThawer.thawBuckets(index, earliestTime, latestTime);
		List<Bucket> thawedBuckets = bucketThawer.getThawedBuckets();
		assertEquals(2, thawedBuckets.size());
		assertTrue(thawedBuckets.contains(thawedBucket1));
		assertTrue(thawedBuckets.contains(thawedBucket2));
	}

	public void getFailedBuckets_whenThawTransferFailExceptionIsThrownForABucket_returnBucket()
			throws ThawTransferFailException, ImportThawedBucketFailException {
		doThrow(ThawTransferFailException.class).when(getsBucketsFromArchive)
				.getBucketFromArchive(bucket);

		run_thawBuckets_bucketFieldPassedToGetsBucketFromArchive();
		List<FailedBucket> failedBuckets = bucketThawer.getFailedBuckets();
		assertEquals(1, failedBuckets.size());
		FailedBucket failedBucket = failedBuckets.get(0);
		assertEquals(bucket, failedBucket.bucket);
		assertTrue(failedBucket.exception instanceof ThawTransferFailException);
	}

	private void run_thawBuckets_bucketFieldPassedToGetsBucketFromArchive() {
		when(
				listsBucketsFiltered.listFilteredBucketsAtIndex(index, earliestTime,
						latestTime)).thenReturn(asList(bucket));
		bucketThawer.thawBuckets(index, earliestTime, latestTime);
	}

	public void getFailedBuckets_whenImportThawedBucketFailExceptionIsThrownForBucket_returnBucket()
			throws ThawTransferFailException, ImportThawedBucketFailException {
		doThrow(ImportThawedBucketFailException.class).when(getsBucketsFromArchive)
				.getBucketFromArchive(bucket);

		run_thawBuckets_bucketFieldPassedToGetsBucketFromArchive();
		List<FailedBucket> failedBuckets = bucketThawer.getFailedBuckets();
		assertEquals(1, failedBuckets.size());
		FailedBucket failedBucket = failedBuckets.get(0);
		assertEquals(bucket, failedBucket.bucket);
		assertTrue(failedBucket.exception instanceof ImportThawedBucketFailException);
	}

	// Sad path

	public void getThawedBuckets_whenThawFails_doesntContainThatBucket()
			throws ThawTransferFailException, ImportThawedBucketFailException {
		doThrow(ThawTransferFailException.class).when(getsBucketsFromArchive)
				.getBucketFromArchive(any(Bucket.class));
		run_thawBuckets_bucketFieldPassedToGetsBucketFromArchive();
		assertTrue(bucketThawer.getThawedBuckets().isEmpty());
	}

	public void getFailedBuckets_whenBucketSucceed_doesntContainThatBucket()
			throws ThawTransferFailException, ImportThawedBucketFailException {
		when(getsBucketsFromArchive.getBucketFromArchive(bucket)).thenReturn(
				mock(LocalBucket.class));
		run_thawBuckets_bucketFieldPassedToGetsBucketFromArchive();
		assertTrue(bucketThawer.getFailedBuckets().isEmpty());
	}
}
