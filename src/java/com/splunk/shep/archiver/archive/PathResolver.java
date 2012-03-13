package com.splunk.shep.archiver.archive;

import java.net.URI;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.fileSystem.WritableFileSystem;
import com.splunk.shep.archiver.model.Bucket;

/**
 * Resolves paths on a {@link ArchiveFileSystem} for buckets.
 */
public class PathResolver {

    private final ArchiveConfiguration configuration;
    private final WritableFileSystem writableFileSystem;

    /**
     * @param configuration
     *            for constructing paths on where to store the buckets.
     * @param writableFileSystem
     *            for getting file system schema and a path which buckets can be
     *            written to, on the file system.
     */
    public PathResolver(ArchiveConfiguration configuration,
	    WritableFileSystem writableFileSystem) {
	this.configuration = configuration;
	this.writableFileSystem = writableFileSystem;
    }

    /**
     * Resolves a bucket's unique path of where to archive the {@link Bucket} on
     * the {@link WritableFileSystem} using configured values of
     * {@link ArchiveConfiguration}
     * 
     * @param bucket
     *            to archive.
     * @return URI to archive the bucket
     */
    public URI resolveArchivePath(Bucket bucket) {
	URI writableUri = writableFileSystem.getWritableUri();
	String archivePathForBucket = getArchivingPath() + "/"
		+ bucket.getIndex() + "/" + bucket.getFormat() + "/"
		+ bucket.getName();
	return URI.create(writableUri + archivePathForBucket);
    }

    /**
     * Returns a path using configured values to where buckets can be archived.
     * Needed to avoid collisions between clusters and servers/indexers.
     * 
     * @return Archiving path that starts with "/"
     */
    private String getArchivingPath() {
	return "/" + configuration.getArchivingRoot() + "/"
		+ configuration.getClusterName() + "/"
		+ configuration.getServerName();
    }

    /**
     * @return Indexes home, which is where on the {@link ArchiveFileSystem}
     *         that you can list indexes.
     */
    public URI getIndexesHome() {
	return URI.create(writableFileSystem.getWritableUri()
		+ getArchivingPath());
    }

    /**
     * @return Buckets home for an index, which is where on the
     *         {@link ArchiveFileSystem} you can list buckets.
     */
    public URI getBucketsHome(String index) {
	return URI.create(getIndexesHome() + "/" + index);
    }
}
