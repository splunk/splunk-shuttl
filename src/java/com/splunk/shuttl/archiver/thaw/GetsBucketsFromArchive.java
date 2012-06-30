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
import com.splunk.shuttl.archiver.model.BucketFactory;

/**
 * Transfers and restores {@link Bucket}s from the archive to the local disk.
 */
public class GetsBucketsFromArchive {

	private static final Logger logger = Logger
			.getLogger(GetsBucketsFromArchive.class);

	private final ThawBucketTransferer thawBucketTransferer;
	private final BucketImporter bucketImporter;
	private final BucketSizeResolver bucketSizeResolver;

	/**
	 * @param thawBucketTransferer
	 * @param bucketImporter
	 * @param bucketSizeResolver
	 */
	public GetsBucketsFromArchive(ThawBucketTransferer thawBucketTransferer,
			BucketImporter bucketImporter, BucketSizeResolver bucketSizeResolver) {
		this.thawBucketTransferer = thawBucketTransferer;
		this.bucketImporter = bucketImporter;
		this.bucketSizeResolver = bucketSizeResolver;
	}

	/**
	 * @return thawed bucket.
	 * @throws ThawTransferFailException
	 *           if the thawing fails.
	 * @throws ImportThawedBucketFailException
	 *           if the import of the thawed bucket fails.
	 */
	public Bucket getBucketFromArchive(Bucket bucket)
			throws ThawTransferFailException, ImportThawedBucketFailException {
		logger.info(will("Attempting to thaw bucket", "bucket", bucket));
		Bucket thawedBucket = getTransferedBucket(bucket);
		Bucket importedBucket = importThawedBucket(thawedBucket);
		Bucket bucketWithSize = bucketSizeResolver.resolveBucketSize(thawedBucket);
		logger.info(done("Thawed bucket", "bucket", importedBucket));
		return BucketFactory.createBucketWithIndexDirectoryAndSize(
				importedBucket.getIndex(), importedBucket.getDirectory(),
				importedBucket.getFormat(), bucketWithSize.getSize());
	}

	private Bucket getTransferedBucket(Bucket bucket)
			throws ThawTransferFailException {
		try {
			return thawBucketTransferer.transferBucketToThaw(bucket);
		} catch (Exception e) {
			logger.error(did("Tried to thaw bucket", e, "Place the bucket in thaw",
					"bucket", bucket, "exception", e));
			throw new ThawTransferFailException(bucket);
		}
	}

	private Bucket importThawedBucket(Bucket thawedBucket)
			throws ImportThawedBucketFailException {
		try {
			return bucketImporter.restoreToSplunkBucketFormat(thawedBucket);
		} catch (Exception e) {
			throw new ImportThawedBucketFailException(thawedBucket);
		}
	}

}
