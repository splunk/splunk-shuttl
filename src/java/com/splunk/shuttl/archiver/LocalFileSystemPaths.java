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
package com.splunk.shuttl.archiver;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;

import javax.management.InstanceNotFoundException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.server.mbeans.ShuttlArchiver;

/**
 * Constants for creating directories where the Archiver can store its locks,
 * unfinished buckets and other files. <br/>
 * Getters that take a {@link Bucket} make sure that the directories returned
 * are unique for that bucket.
 */
public class LocalFileSystemPaths {

	final String SAFE_BUCKETS_NAME = "safe-buckets";

	final String FAILED_BUCKETS_NAME = "failed-buckets";

	final String ARCHIVE_LOCKS_NAME = "archive-locks-dir";

	final String EXPORT_DIR_NAME = "format-export-dir";

	final String THAW_LOCKS_NAME = "thaw-locks-dir";

	final String THAW_TRANSFERS_NAME = "thaw-transfers-dir";

	final String METADATA_DIR_NAME = "metadata-dir";

	final String METADATA_TRANSFERS_NAME = "metadata-transfers-dir";

	final String COPY_RECEIPTS_NAME = "copy-receipts-dir";

	final String COPY_LOCKS_NAME = "copy-locks-dir";

	final String PUT_TRANSFER_LOCKS_NAME = "put-transfers-locks-dir";

	private final String archiverDirectoryPath;

	public LocalFileSystemPaths(File directory) {
		this(directory.getAbsolutePath());
	}

	public LocalFileSystemPaths(String archiverDirectoryPath) {
		this.archiverDirectoryPath = archiverDirectoryPath;
	}

	/**
	 * Directory which contains all files created by the archiver.
	 */
	public File getArchiverDirectory() {
		String tildeAdjustedPath = archiverDirectoryPath.replace("~",
				FileUtils.getUserDirectoryPath());
		String pathWithoutTildeNorFileSchema = removeEventualFileSchema(tildeAdjustedPath);
		return new File(pathWithoutTildeNorFileSchema, "data");
	}

	private String removeEventualFileSchema(String path) {
		if (path.startsWith("file:/"))
			return path.replaceFirst("file:/", "");
		return path;
	}

	/**
	 * Safe location for the buckets to be archived. Stored away from Splunk,
	 * where Splunk cannot delete the buckets.
	 */
	public File getSafeDirectory() {
		return createDirectoryUnderArchiverDir(SAFE_BUCKETS_NAME);
	}

	private File createDirectoryUnderArchiverDir(String name) {
		File dir = new File(getArchiverDirectory(), name);
		dir.mkdirs();
		return dir;
	}

	/**
	 * Contains the failed bucket archiving transfers
	 */
	public File getFailDirectory() {
		return createDirectoryUnderArchiverDir(FAILED_BUCKETS_NAME);
	}

	/**
	 * Contains locks for archiving buckets. Unique path for each bucket within a
	 * Splunk indexer.
	 */
	public File getArchiveLocksDirectory(Bucket bucket) {
		return createBucketUniqueDirUnderArchiverDir(ARCHIVE_LOCKS_NAME, bucket);
	}

	private File createBucketUniqueDirUnderArchiverDir(String name, Bucket bucket) {
		File directoryUnderArchiverDir = createDirectoryUnderArchiverDir(name);
		File indexDir = new File(directoryUnderArchiverDir, bucket.getIndex());
		File bucketNameDir = new File(indexDir, bucket.getName());
		File formatDir = new File(bucketNameDir, bucket.getFormat().toString());
		formatDir.mkdirs();
		return formatDir;
	}

	/**
	 * Contains files required when exporting a bucket to a new format. Unique
	 * path for each bucket within a Splunk indexer.
	 */
	public File getExportDirectory(Bucket bucket) {
		return createBucketUniqueDirUnderArchiverDir(EXPORT_DIR_NAME, bucket);
	}

	/**
	 * Contains locks for thawing buckets. Unique path for each bucket within a
	 * Splunk indexer.
	 */
	public File getThawLocksDirectory(Bucket bucket) {
		return createBucketUniqueDirUnderArchiverDir(THAW_LOCKS_NAME, bucket);
	}

	/**
	 * The parent of all the thaw locks for the buckets. @see
	 * {@link LocalFileSystemPaths#getThawLocksDirectory(Bucket)}
	 */
	public File getThawLocksDirectoryForAllBuckets() {
		return createDirectoryUnderArchiverDir(THAW_LOCKS_NAME);
	}

	/**
	 * Temporary contains thaw transfers. Unique path for each bucket within a
	 * Splunk indexer.
	 */
	public File getThawTransfersDirectory(Bucket bucket) {
		return createBucketUniqueDirUnderArchiverDir(THAW_TRANSFERS_NAME, bucket);
	}

	/**
	 * The parent of all the thaw transfers. @see
	 * {@link LocalFileSystemPaths#getThawTransfersDirectory(Bucket)}
	 */
	public File getThawTransfersDirectoryForAllBuckets() {
		return createDirectoryUnderArchiverDir(THAW_TRANSFERS_NAME);
	}

	/**
	 * Directory for bucket metadata that the archiver adds to a bucket. Unique
	 * path for each bucket within a Splunk indexer.
	 */
	public File getMetadataDirectory(Bucket bucket) {
		return createBucketUniqueDirUnderArchiverDir(METADATA_DIR_NAME, bucket);
	}

	/**
	 * Directory for transferring metadata. Will be unique for the bucket.
	 */
	public File getMetadataTransfersDirectory(Bucket bucket) {
		return createBucketUniqueDirUnderArchiverDir(METADATA_TRANSFERS_NAME,
				bucket);
	}

	public File getCopyBucketReceiptsDirectory(Bucket bucket) {
		return createBucketUniqueDirUnderArchiverDir(COPY_RECEIPTS_NAME, bucket);
	}

	public File getCopyLocksDirectory(Bucket bucket) {
		return createBucketUniqueDirUnderArchiverDir(COPY_LOCKS_NAME, bucket);
	}

	public File getTransferLocksDirectory(Bucket bucket) {
		return createBucketUniqueDirUnderArchiverDir(PUT_TRANSFER_LOCKS_NAME,
				bucket);
	}

	public static LocalFileSystemPaths create() {
		String archiverPath = getPathForArchiverData();
		return new LocalFileSystemPaths(archiverPath);
	}

	private static String getPathForArchiverData() {
		try {
			return ShuttlArchiver.getMBeanProxy().getLocalArchiverDir();
		} catch (InstanceNotFoundException e) {
			Logger.getLogger(LocalFileSystemPaths.class).error(
					did("Tried getting the MBean for ShuttlArchiver", e,
							"To get the MBean", "exception", e));
			throw new ArchiverMBeanNotRegisteredException(e);
		}
	}

	/**
	 * @param create
	 *          - with config.
	 */
	public static LocalFileSystemPaths create(ArchiveConfiguration config) {
		return new LocalFileSystemPaths(config.getLocalArchiverDir());
	}
}
