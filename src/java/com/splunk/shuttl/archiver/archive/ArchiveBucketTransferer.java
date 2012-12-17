// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.filesystem.transaction.Transaction;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionException;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionExecuter;
import com.splunk.shuttl.archiver.filesystem.transaction.bucket.PutBucketTransaction;
import com.splunk.shuttl.archiver.metastore.ArchiveBucketSize;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Class for transferring buckets
 */
public class ArchiveBucketTransferer {

	private final static Logger logger = Logger
			.getLogger(ArchiveBucketTransferer.class);

	private final ArchiveFileSystem archiveFileSystem;
	private final PathResolver pathResolver;
	private final ArchiveBucketSize archiveBucketSize;
	private final TransactionExecuter transactionExecuter;

	public ArchiveBucketTransferer(ArchiveFileSystem archive,
			PathResolver pathResolver, ArchiveBucketSize archiveBucketSize,
			TransactionExecuter transactionExecuter) {
		this.archiveFileSystem = archive;
		this.pathResolver = pathResolver;
		this.archiveBucketSize = archiveBucketSize;
		this.transactionExecuter = transactionExecuter;
	}

	/**
	 * Transfers the bucket and its content to the archive.
	 * 
	 * @param bucket
	 *          to transfer to {@link ArchiveFileSystem}
	 * @throws FailedToArchiveBucketException
	 *           if bucket failed to be transfered to the archive for any reason.
	 */
	public void transferBucketToArchive(Bucket bucket) {
		String destination = pathResolver.resolveArchivePath(bucket);
		String tempPath = pathResolver.resolveTempPathForBucket(bucket);
		logger.info(will("attempting to transfer bucket to archive", "bucket",
				bucket, "destination", destination));
		Transaction bucketTransaction = PutBucketTransaction.create(
				archiveFileSystem, bucket, tempPath, destination);

		// TODO: Merge the bucket transaction and the bucketsize transaction. They
		// should be able to be run at once with
		// transactionExecuter.execute(Transaction... transactions)
		bucketTransaction(bucket, bucketTransaction);
		bucketSizeTransaction(bucket);
	}

	private void bucketTransaction(Bucket bucket, Transaction bucketTransaction) {
		try {
			transactionExecuter.execute(bucketTransaction);
		} catch (TransactionException e) {
			logger.error(did("Executed a bucket transaction.", e,
					"To transfer the bucket to the archive.", "bucket", bucket));
			throw new FailedToArchiveBucketException(e);
		}
	}

	private void bucketSizeTransaction(Bucket bucket) {
		archiveBucketSize.persistBucketSize(bucket);
	}

	/**
	 * This method exists since a {@link Bucket} can be archived with multiple
	 * formats. A Bucket may have to be re-transmitted after a failed archiving
	 * attempt. And since buckets can exist in different formats, one format may
	 * have successfully been archived while another format failed.<br/>
	 * This method can be used to test if the bucket in a specific format has been
	 * successfully transfered.
	 * 
	 * @return true if the {@link Bucket} in {@link BucketFormat} is archived.
	 */
	public boolean isArchived(Bucket bucket, BucketFormat format) {
		String bucketPathWithFormat = pathResolver.resolveArchivedBucketPath(
				bucket.getIndex(), bucket.getName(), format);
		return !listPathsForBucketPath(bucketPathWithFormat).isEmpty();
	}

	private List<String> listPathsForBucketPath(String bucketPathWithFormat) {
		try {
			return archiveFileSystem.listPath(bucketPathWithFormat);
		} catch (IOException e) {
			logIOException(bucketPathWithFormat, e);
			throw new RuntimeException(e);
		}
	}

	private void logIOException(String bucketPathWithFormat, IOException e) {
		Logger.getLogger(getClass()).error(
				did("Listed path in the archive with path: " + bucketPathWithFormat, e,
						"To list files at path", "path", bucketPathWithFormat, "exception",
						e));
	}
}
