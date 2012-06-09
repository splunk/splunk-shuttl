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

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.importexport.BucketExporter;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Archives buckets the way that it is configured to archive them.
 */
public class BucketArchiver {

	private final BucketExporter bucketExporter;
	private final ArchiveBucketTransferer archiveBucketTransferer;
	private final BucketDeleter bucketDeleter;

	/**
	 * Constructor following dependency injection pattern, makes it easier to
	 * test.<br/>
	 * Use {@link BucketArchiverFactory} for creating a {@link BucketArchiver}.
	 * 
	 * @param exporter
	 *          to export the bucket
	 * @param archiveBucketTransferer
	 *          to transfer the bucket to an {@link ArchiveFileSystem}
	 * @param bucketDeleter
	 *          that deletesBuckets that has been archived.
	 */
	public BucketArchiver(BucketExporter exporter,
			ArchiveBucketTransferer archiveBucketTransferer,
			BucketDeleter bucketDeleter) {
		this.bucketExporter = exporter;
		this.archiveBucketTransferer = archiveBucketTransferer;
		this.bucketDeleter = bucketDeleter;
	}

	public void archiveBucket(Bucket bucket) {
		Bucket exportedBucket = bucketExporter.exportBucket(bucket);
		archiveThenDeleteExportedBucket(exportedBucket);
		bucketDeleter.deleteBucket(bucket);
	}

	private void archiveThenDeleteExportedBucket(Bucket exportedBucket) {
		try {
			archiveBucketTransferer.transferBucketToArchive(exportedBucket);
		} finally {
			bucketDeleter.deleteBucket(exportedBucket);
		}
	}
}
