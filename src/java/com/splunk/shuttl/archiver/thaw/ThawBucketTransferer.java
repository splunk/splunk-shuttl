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

import java.io.File;
import java.io.IOException;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.transaction.Transaction;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionExecuter;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionProvider;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Transfers bucket to thaw.
 */
public class ThawBucketTransferer {

	private final ThawLocationProvider thawLocationProvider;
	private final ArchiveFileSystem archiveFileSystem;
	private final BucketFactory bucketFactory;
	private TransactionExecuter transactionExecuter;

	public ThawBucketTransferer(ThawLocationProvider thawLocationProvider,
			ArchiveFileSystem archiveFileSystem, BucketFactory bucketFactory,
			TransactionExecuter transactionExecuter) {
		this.thawLocationProvider = thawLocationProvider;
		this.archiveFileSystem = archiveFileSystem;
		this.bucketFactory = bucketFactory;
		this.transactionExecuter = transactionExecuter;
	}

	/**
	 * Transfers an archived bucket in the thaw directory of the bucket's index.
	 * 
	 * @return the transferred bucket.
	 */
	public LocalBucket transferBucketToThaw(Bucket bucket) throws IOException {
		File temp = thawLocationProvider.getThawTransferLocation(bucket);
		File dst = thawLocationProvider.getLocationInThawForBucket(bucket);
		Transaction getBucketTransaction = TransactionProvider.createGet(
				archiveFileSystem, bucket, temp.getAbsolutePath(),
				dst.getAbsolutePath());
		transactionExecuter.execute(getBucketTransaction);

		return bucketFactory.createWithIndexDirectoryAndSize(bucket.getIndex(),
				dst, bucket.getFormat(), bucket.getSize());
	}
}
