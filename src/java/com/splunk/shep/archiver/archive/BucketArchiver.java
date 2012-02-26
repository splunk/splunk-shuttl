package com.splunk.shep.archiver.archive;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.fileSystem.FileSystemPath;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.server.mbeans.rest.BucketArchiverRest;

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
     * test.
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
    protected BucketArchiver(ArchiveConfiguration config,
	    BucketExporter exporter, PathResolver pathResolver,
	    BucketTransferer bucketTransferer) {
	this.archiveConfiguration = config;
	this.bucketExporter = exporter;
	this.pathResolver = pathResolver;
	this.bucketTransferer = bucketTransferer;
    }

    public void archiveBucket(Bucket bucket) {
	ArchiveFormat archiveFormat = archiveConfiguration.getArchiveFormat();
	bucketExporter.exportBucketToFormat(bucket, archiveFormat);
	FileSystemPath path = pathResolver
		.resolveArchivePathWithBucketAndFormat(bucket, archiveFormat);
	bucketTransferer.transferBucketToPath(bucket, path);
    }

    /**
     * The REST end point {@link BucketArchiverRest} needs to be able to create
     * {@link BucketArchiver} without any arguments.
     * 
     * @return default setup of the BucketArchiver that is configured by
     *         configuration files.
     */
    public static BucketArchiver create() {
	return new BucketArchiver(new ArchiveConfiguration(),
		new BucketExporter(), new PathResolver(),
		new BucketTransferer());
    }

}
