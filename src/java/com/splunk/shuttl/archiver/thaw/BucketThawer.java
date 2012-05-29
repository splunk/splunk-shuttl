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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.splunk.shuttl.archiver.fileSystem.FileOverwriteException;
import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.listers.ArchiveBucketsLister;
import com.splunk.shuttl.archiver.listers.ListsBucketsFiltered;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.IllegalIndexException;

/**
 * Interacts with the archive to thaw buckets within the users needs, which is
 * currently only a time range.
 */
public class BucketThawer {

	private final ListsBucketsFiltered listsBucketsFiltered;
	private final GetsBucketsFromArchive getsBucketsFromArchive;

	public static class ThawInfo {

		public final Bucket bucket;
		public final Status status;
		private final Exception exception;

		public ThawInfo(Bucket bucket, Status status, Exception exception) {
			this.bucket = bucket;
			this.status = status;
			this.exception = exception;
		}

		public enum Status {
			THAWED, FAILED
		}

		/**
		 * @return message from exception
		 */
		public String getExceptionMessage() {
			if (exception != null)
				if (exception instanceof FileOverwriteException)
					return "Directory already exists - bucket is probably already thawed";
				else if (exception instanceof IllegalIndexException)
					return "Given index does not exist in running Splunk instance";
				else if (exception instanceof IOException)
					return exception.getMessage();
				else
					return "Unexpected exception: " + exception.getMessage();
			return "Did not get an exception when thawing.";
		};

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
		this.listsBucketsFiltered = new ListsBucketsFiltered(bucketsLister,
				bucketFilter, bucketFormatResolver);
		this.getsBucketsFromArchive = new GetsBucketsFromArchive(
				thawBucketTransferer, bucketImporter);
	}

	/**
	 * Thaw buckets by listing buckets in an index, filter the buckets, resolve
	 * their formats and lastly transferring them to the thaw directory.
	 * 
	 * @param index
	 *          The index to thaw from. If null, all existing indexes are thawed
	 */
	public List<ThawInfo> thawBuckets(String index, Date earliestTime,
			Date latestTime) {
		List<Bucket> bucketsToThaw = getBucketsToThaw(index, earliestTime,
				latestTime);
		return thawBuckets(bucketsToThaw);
	}

	private List<Bucket> getBucketsToThaw(String index, Date earliestTime,
			Date latestTime) {
		if (index == null)
			return listsBucketsFiltered.listFilteredBuckets(earliestTime, latestTime);
		else
			return listsBucketsFiltered.listFilteredBucketsAtIndex(index,
					earliestTime, latestTime);
	}

	private List<ThawInfo> thawBuckets(List<Bucket> bucketsWithFormats) {
		List<ThawInfo> thawInfos = new ArrayList<ThawInfo>();

		for (Bucket bucket : bucketsWithFormats)
			thawInfos.add(getsBucketsFromArchive.getBucketFromArchive(bucket));

		return thawInfos;
	}

}
