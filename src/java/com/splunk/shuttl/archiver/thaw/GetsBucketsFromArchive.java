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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.thaw.BucketThawer.ThawInfo;

/**
 * Transfers and restores {@link Bucket}s from the archive to the local disk.
 */
public class GetsBucketsFromArchive {

	private static final Logger logger = Logger
			.getLogger(GetsBucketsFromArchive.class);

	private final ThawBucketTransferer thawBucketTransferer;
	private final BucketImporter bucketImporter;

	/**
	 * @param thawBucketTransferer
	 * @param bucketImporter
	 */
	public GetsBucketsFromArchive(ThawBucketTransferer thawBucketTransferer,
			BucketImporter bucketImporter) {
		this.thawBucketTransferer = thawBucketTransferer;
		this.bucketImporter = bucketImporter;
	}

	/**
	 * @param bucket
	 * @return
	 */
	public ThawInfo getBucketFromArchive(Bucket bucket) {
		logger.info(will("Attempting to thaw bucket", "bucket", bucket));
		ThawInfo thawInfo = null;
		try {
			Bucket thawedBucket = thawBucketTransferer.transferBucketToThaw(bucket);
			Bucket thawedImportedBucket = bucketImporter
					.restoreToSplunkBucketFormat(thawedBucket);
			logger.info(done("Thawed bucket", "bucket", thawedImportedBucket));
		} catch (Exception e) {
			// TODO: Thawing could be sped up by ignoring buckets in indexes
			// we know don't exist
			logger.error(did("Tried to thaw bucket", e, "Place the bucket in thaw",
					"bucket", bucket, "exception", e));
			thawInfo = new ThawInfo(bucket, ThawInfo.Status.FAILED, e);
		}
		return thawInfo;
	}

}
