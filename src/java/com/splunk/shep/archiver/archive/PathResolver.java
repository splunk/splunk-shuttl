package com.splunk.shep.archiver.archive;

import java.net.URI;

import org.apache.commons.io.FilenameUtils;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.util.UtilsURI;

/**
 * Resolves paths on a {@link ArchiveFileSystem} for buckets.
 */
public class PathResolver {

    public static final char SEPARATOR = '/';

    private final ArchiveConfiguration configuration;

    /**
     * @param configuration
     *            for constructing paths on where to store the buckets.
     */
    public PathResolver(ArchiveConfiguration configuration) {
	this.configuration = configuration;
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
	String archivePathForBucket = getArchivingPath().toString() + SEPARATOR
		+ bucket.getIndex() + SEPARATOR + bucket.getName() + SEPARATOR
		+ bucket.getFormat();
	return URI.create(archivePathForBucket);
    }

    /**
     * Returns a path using configured values to where buckets can be archived.
     * Needed to avoid collisions between clusters and servers/indexers.
     * 
     * @return Archiving path that starts with "/"
     */
    private URI getArchivingPath() {
	return URI.create(configuration.getArchivingRoot().toString()
		+ SEPARATOR + configuration.getClusterName() + SEPARATOR
		+ configuration.getServerName());
    }

    /**
     * @return Indexes home, which is where on the {@link ArchiveFileSystem}
     *         that you can list indexes.
     */
    public URI getIndexesHome() {
	return getArchivingPath();
    }

    /**
     * @return Buckets home for an index, which is where on the
     *         {@link ArchiveFileSystem} you can list buckets.
     */
    public URI getBucketsHome(String index) {
	return URI.create(getIndexesHome().toString() + SEPARATOR + index);
    }

    /**
     * Resolves index from a {@link URI} to a bucket.<br/>
     * <br/>
     * 
     * @param bucketURI
     *            , {@link URI} needs to have the index in a structure decided
     *            by a {@link PathResolver}.
     */
    public String resolveIndexFromUriToBucket(URI bucketURI) {
	String bucketPath = UtilsURI
		.getPathByTrimmingEndingFileSeparator(bucketURI);
	String parentWhichIsIndex = getParent(bucketPath);
	return FilenameUtils.getBaseName(parentWhichIsIndex);
    }

    private String getParent(String bucketPath) {
	return FilenameUtils.getPathNoEndSeparator(bucketPath);
    }

    /**
     * @return {@link URI} to where formats can be listed for a bucket.
     */
    public URI getFormatsHome(String index, String bucketName) {
	return URI.create(getBucketsHome(index).toString() + SEPARATOR
		+ bucketName);
    }

    /**
     * {@link URI} to an archived bucket.
     * 
     * @param index
     *            to the bucket
     * @param bucketName
     *            of the bucket
     * @param format
     *            of the bucket
     * @return {@link URI} to archived bucket.
     */
    public URI resolveArchivedBucketURI(String index, String bucketName,
	    BucketFormat format) {
	return URI.create(getFormatsHome(index, bucketName).toString()
		+ SEPARATOR + format);
    }

}
