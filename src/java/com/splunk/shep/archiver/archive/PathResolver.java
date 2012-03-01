package com.splunk.shep.archiver.archive;

import java.net.URI;

import com.splunk.shep.archiver.fileSystem.WritableFileSystem;
import com.splunk.shep.archiver.model.Bucket;

/**
 * Resolves paths on a file system for buckets.
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
	String uri = configuration.getArchivingRoot() + "/"
		+ configuration.getClusterName() + "/"
		+ configuration.getServerName() + "/" + bucket.getIndex() + "/"
		+ bucket.getFormat() + "/" + bucket.getName();
	return URI.create(writableUri + "/" + uri);
    }
}
