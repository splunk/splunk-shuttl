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
package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketArchiverRunner.BucketShuttler;
import com.splunk.shuttl.archiver.importexport.BucketExportController;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Copies a bucket in all formats with the {@link ArchiveBucketTransferer}.
 */
public class BucketCopier implements BucketShuttler {

	private static final Logger logger = Logger.getLogger(BucketCopier.class);

	private final BucketExportController bucketExportController;
	private final ArchiveBucketTransferer archiveBucketTransferer;
	private final List<BucketFormat> bucketFormats;
	private final BucketDeleter bucketDeleter;

	public BucketCopier(BucketExportController bucketExportController,
			ArchiveBucketTransferer archiveBucketTransferer,
			List<BucketFormat> bucketFormats, BucketDeleter bucketDeleter) {
		this.bucketExportController = bucketExportController;
		this.archiveBucketTransferer = archiveBucketTransferer;
		this.bucketFormats = bucketFormats;
		this.bucketDeleter = bucketDeleter;
	}

	public void copyBucket(LocalBucket bucket) {
		List<RuntimeException> copyExceptions = new ArrayList<RuntimeException>();
		for (BucketFormat format : bucketFormats)
			if (!archiveBucketTransferer.isArchived(bucket, format))
				exportBucketThenCopy(bucket, format, copyExceptions);

		if (!copyExceptions.isEmpty())
			throw new RuntimeException("Got some exceptions when copying bucket: "
					+ copyExceptions.toString());
	}

	private void exportBucketThenCopy(LocalBucket bucket, BucketFormat format,
			List<RuntimeException> copyExceptions) {
		LocalBucket exportedBucket = bucketExportController.exportBucket(bucket,
				format);
		try {
			archiveBucketTransferer.transferBucketToArchive(exportedBucket);
		} catch (RuntimeException e) {
			logException(exportedBucket, e);
			copyExceptions.add(e);
		} finally {
			if (!bucket.equals(exportedBucket))
				bucketDeleter.deleteBucket(exportedBucket);
		}
	}

	private void logException(LocalBucket exportedBucket, RuntimeException e) {
		logger.debug(warn("Copied bucket", e,
				"Will eventually throw this exception", "bucket", exportedBucket));
	}

	@Override
	public void shuttlBucket(LocalBucket bucket) {
		copyBucket(bucket);
	}
}
