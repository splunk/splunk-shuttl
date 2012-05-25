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

import java.net.URI;

import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Archives buckets the way that it is configured to archive them.
 */
public class BucketArchiver {

    private final ArchiveConfiguration archiveConfiguration;
    private final BucketExporter bucketExporter;
    private final PathResolver pathResolver;
    private final ArchiveBucketTransferer archiveBucketTransferer;
    private final BucketDeleter bucketDeleter;

    /**
     * Constructor following dependency injection pattern, makes it easier to
     * test.<br/>
     * Use {@link BucketArchiverFactory} for creating a {@link BucketArchiver}.
     * 
     * @param config
     *            to be used by the archiver
     * @param exporter
     *            to export the bucket
     * @param pathResolver
     *            to resolve archive paths for the buckets
     * @param archiveBucketTransferer
     *            to transfer the bucket to an {@link ArchiveFileSystem}
     * @param bucketDeleter
     *            that deletesBuckets that has been archived.
     */
    /* package-private */BucketArchiver(ArchiveConfiguration config,
	    BucketExporter exporter, PathResolver pathResolver,
	    ArchiveBucketTransferer archiveBucketTransferer,
	    BucketDeleter bucketDeleter) {
	this.archiveConfiguration = config;
	this.bucketExporter = exporter;
	this.pathResolver = pathResolver;
	this.archiveBucketTransferer = archiveBucketTransferer;
	this.bucketDeleter = bucketDeleter;
    }

    public void archiveBucket(Bucket bucket) {
	BucketFormat bucketFormat = archiveConfiguration.getArchiveFormat();
	Bucket exportedBucket = bucketExporter.exportBucketToFormat(bucket,
		bucketFormat);
	archiveThenDeleteExportedBucket(exportedBucket);
	bucketDeleter.deleteBucket(bucket);
    }

    private void archiveThenDeleteExportedBucket(Bucket exportedBucket) {
	try {
	    archiveExportedBucket(exportedBucket);
	} finally {
	    bucketDeleter.deleteBucket(exportedBucket);
	}
    }

    private void archiveExportedBucket(Bucket bucket) {
	URI path = pathResolver.resolveArchivePath(bucket);
	archiveBucketTransferer.transferBucketToArchive(bucket, path);
    }

    /**
     * Used to clean up the archived buckets when testing.
     * 
     * @return {@link PathResolver} for the {@link BucketArchiver}.
     */
    public PathResolver getPathResolver() {
	return pathResolver;
    }
}
