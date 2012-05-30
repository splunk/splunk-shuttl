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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.listers.ArchiveBucketsLister;
import com.splunk.shuttl.archiver.listers.ListsBucketsFiltered;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Interacts with the archive to thaw buckets within the users needs, which is
 * currently only a time range.
 */
public class BucketThawer {

	private final ListsBucketsFiltered listsBucketsFiltered;
	private final GetsBucketsFromArchive getsBucketsFromArchive;
	private final List<Bucket> successfulThawedBuckets;
	private final List<FailedBucket> failedBuckets;

	public static class FailedBucket {

		public final Bucket bucket;
		public final Exception exception;

		public FailedBucket(Bucket bucket, Exception exception) {
			this.bucket = bucket;
			this.exception = exception;
		}

	}

	/**
	 * @param bucketsLister
	 *          used for listing buckets in the archive.
	 * @param bucketFilter
	 *          filtering buckets to only get the buckets that satisfies the
	 *          thawing needs.
	 * @param bucketFormatResolver
	 *          to resolve the format to thaw for the bucket.
	 * @param thawBucketTransferer
	 *          for transferring the buckets to thawed.
	 * @param bucketImporter
	 */
	public BucketThawer(ArchiveBucketsLister bucketsLister,
			BucketFilter bucketFilter, BucketFormatResolver bucketFormatResolver,
			ThawBucketTransferer thawBucketTransferer, BucketImporter bucketImporter) {
		this(new ListsBucketsFiltered(bucketsLister, bucketFilter,
				bucketFormatResolver), new GetsBucketsFromArchive(thawBucketTransferer,
				bucketImporter));
	}

	/**
	 * @param listsBucketsFiltered2
	 * @param getsBucketsFromArchive2
	 */
	public BucketThawer(ListsBucketsFiltered listsBucketsFiltered,
			GetsBucketsFromArchive getsBucketsFromArchive) {
		this.listsBucketsFiltered = listsBucketsFiltered;
		this.getsBucketsFromArchive = getsBucketsFromArchive;
		this.successfulThawedBuckets = new ArrayList<Bucket>();
		this.failedBuckets = new ArrayList<FailedBucket>();
	}

	/**
	 * Thaws bucket for a specific index within a time range.
	 */
	public void thawBuckets(String index, Date earliestTime, Date latestTime) {
		List<Bucket> bucketsToThaw = listsBucketsFiltered
				.listFilteredBucketsAtIndex(index, earliestTime, latestTime);
		for (Bucket bucket : bucketsToThaw)
			getThawedBucketFromArchive(bucket);
	}

	private void getThawedBucketFromArchive(Bucket bucket) {
		try {
			Bucket thawedBucket = getsBucketsFromArchive.getBucketFromArchive(bucket);
			successfulThawedBuckets.add(thawedBucket);
		} catch (ThawTransferFailException e) {
			failedBuckets.add(new FailedBucket(bucket, e));
		} catch (ImportThawedBucketFailException e) {
			failedBuckets.add(new FailedBucket(bucket, e));
		}
	}

	/**
	 * @return buckets that succeeded to be thawed.
	 */
	public List<Bucket> getThawedBuckets() {
		return successfulThawedBuckets;
	}

	/**
	 * @return buckets that failed to be thawed.
	 */
	public List<FailedBucket> getFailedBuckets() {
		return failedBuckets;
	}

}
