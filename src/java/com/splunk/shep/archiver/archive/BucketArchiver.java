package com.splunk.shep.archiver.archive;

import java.net.URI;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.model.Bucket;

/**
 * Archives buckets the way that it is configured to archive them.
 */
public class BucketArchiver {

    private final ArchiveConfiguration archiveConfiguration;
    private final BucketExporter bucketExporter;
    private final PathResolver pathResolver;
    private final BucketTransferer bucketTransferer;

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
     * @param bucketTransferer
     *            to transfer the bucket to an {@link ArchiveFileSystem}
     */
    /* package-private */BucketArchiver(ArchiveConfiguration config,
	    BucketExporter exporter, PathResolver pathResolver,
	    BucketTransferer bucketTransferer) {
	this.archiveConfiguration = config;
	this.bucketExporter = exporter;
	this.pathResolver = pathResolver;
	this.bucketTransferer = bucketTransferer;
    }

    public void archiveBucket(Bucket bucket) {
	BucketFormat bucketFormat = archiveConfiguration.getArchiveFormat();
	Bucket exportedBucket = bucketExporter.getBucketExportedToFormat(
		bucket, bucketFormat);
	URI path = pathResolver.resolveArchivePath(exportedBucket);
	bucketTransferer.transferBucketToArchive(bucket, path);
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
