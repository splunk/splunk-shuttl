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

import org.apache.commons.io.FilenameUtils;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.util.UtilsURI;

/**
 * Resolves paths on a {@link ArchiveFileSystem} for buckets.
 */
public class PathResolver {

	public static final char SEPARATOR = '/';
	private static final String bucketSizeSuffix = SEPARATOR + "archive_meta"
			+ SEPARATOR + "bucket.size";

	private final ArchiveConfiguration configuration;

	/**
	 * @param configuration
	 *          for constructing paths on where to store the buckets.
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
	 *          to archive.
	 * @return Path to archive the bucket
	 */
	public String resolveArchivePath(Bucket bucket) {
		String archivePathForBucket = getArchivingPath() + SEPARATOR
				+ bucket.getIndex() + SEPARATOR + bucket.getName() + SEPARATOR
				+ bucket.getFormat();
		return archivePathForBucket;
	}

	/**
	 * Returns a path using configured values to where buckets can be archived.
	 * Needed to avoid collisions between clusters and servers/indexers.
	 * 
	 * @return Archiving path that starts with "/"
	 */
	private String getArchivingPath() {
		return configuration.getArchivePath() + SEPARATOR
				+ configuration.getClusterName() + SEPARATOR
				+ configuration.getServerName();
	}

	/**
	 * @return Indexes home, which is where on the {@link ArchiveFileSystem} that
	 *         you can list indexes.
	 */
	public String getIndexesHome() {
		return getArchivingPath();
	}

	/**
	 * @return Buckets home for an index, which is where on the
	 *         {@link ArchiveFileSystem} you can list buckets.
	 */
	public String getBucketsHome(String index) {
		return getIndexesHome() + SEPARATOR + index;
	}

	/**
	 * Resolves index from a Path to a bucket.<br/>
	 * <br/>
	 * 
	 * @param bucketPath
	 *          , Path needs to have the index in a structure decided by a
	 *          {@link PathResolver}.
	 */
	public String resolveIndexFromPathToBucket(String bucketPath) {
		String parentWhichIsIndex = getParent(UtilsURI
				.getPathByTrimmingEndingFileSeparator(bucketPath));
		return FilenameUtils.getBaseName(parentWhichIsIndex);
	}

	private String getParent(String bucketPath) {
		return FilenameUtils.getPathNoEndSeparator(bucketPath);
	}

	/**
	 * @return Path to where formats can be listed for a bucket.
	 */
	public String getFormatsHome(String index, String bucketName) {
		return getBucketsHome(index) + SEPARATOR + bucketName;
	}

	/**
	 * Path to an archived bucket.
	 * 
	 * @param index
	 *          to the bucket
	 * @param bucketName
	 *          of the bucket
	 * @param format
	 *          of the bucket
	 * @return Path to archived bucket.
	 */
	public String resolveArchivedBucketPath(String index, String bucketName,
			BucketFormat format) {
		return getFormatsHome(index, bucketName) + SEPARATOR + format;
	}

	/**
	 * @return a {@link PathResolver} configured with
	 *         {@link ArchiverConfiguration}.
	 */
	public static PathResolver getConfigured() {
		ArchiveConfiguration archiveConfiguration = ArchiveConfiguration
				.getSharedInstance();
		return new PathResolver(archiveConfiguration);
	}

	/**
	 * @return Path to where a bucket's file with local disk size information.
	 */
	public String getBucketSizeFilePathForBucket(Bucket bucket) {
		return resolveArchivePath(bucket) + bucketSizeSuffix;
	}

	/**
	 * @return Path to a temporary location on the {@link ArchiveFileSystem} where
	 *         it can be transferred to, and yet be "invisible" from the system.
	 */
	public String resolveTempPathForBucket(Bucket bucket) {
		return configuration.getTmpDirectory() + resolveArchivePath(bucket);
	}

	/**
	 * @return Path to a temporary location for the bucket size. @see
	 *         {@link PathResolver#resolveTempPathForBucketSize(Bucket)}.
	 */
	public String resolveTempPathForBucketSize(Bucket bucket) {
		String tempPathForBucket = resolveTempPathForBucket(bucket);
		return tempPathForBucket + bucketSizeSuffix;
	}
}
