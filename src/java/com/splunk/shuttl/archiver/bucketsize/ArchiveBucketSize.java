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
package com.splunk.shuttl.archiver.bucketsize;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.transaction.Transaction;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionException;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionExecuter;
import com.splunk.shuttl.archiver.filesystem.transaction.file.GetFileTransaction;
import com.splunk.shuttl.archiver.filesystem.transaction.file.PutFileTransaction;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Gets a {@link Bucket}'s size. Both a remote bucket and a local bucket. <br/>
 * <br/>
 * This is needed because we want to know how big the {@link Bucket} will be on
 * the local file system and there's no guarantee that the size on the archive
 * file system is the same as on local disk. We therefore need to be able to put
 * and get the local file system size of the bucket, from the archive file
 * system.
 */
public class ArchiveBucketSize {

	private final Logger logger = Logger.getLogger(ArchiveBucketSize.class);

	private final PathResolver pathResolver;
	private final BucketSizeIO bucketSizeIO;
	private final ArchiveFileSystem archiveFileSystem;
	private final FlatFileStorage flatFileStorage;
	private final LocalFileSystemPaths localFileSystemPaths;

	public ArchiveBucketSize(PathResolver pathResolver,
			BucketSizeIO bucketSizeIO, ArchiveFileSystem archiveFileSystem,
			FlatFileStorage flatFileStorage, LocalFileSystemPaths localFileSystemPaths) {
		this.pathResolver = pathResolver;
		this.bucketSizeIO = bucketSizeIO;
		this.archiveFileSystem = archiveFileSystem;
		this.flatFileStorage = flatFileStorage;
		this.localFileSystemPaths = localFileSystemPaths;
	}

	/**
	 * @return size of an archived bucket on the local file system. Returns null
	 *         if the archiveSize is not persisted locally nor remotely.
	 */
	public Long getSize(Bucket bucket) {
		File metadataFile = flatFileStorage.getFlatFile(bucket,
				bucketSizeIO.getSizeMetadataFileName());
		Long size;
		try {
			getRemoteFileIfNeeded(bucket, metadataFile);
		} catch (TransactionException e) {
			logger.warn(warn("Tried getting the remote metadata: " + metadataFile, e,
					"Will return null instead of the real metadata", "file",
					metadataFile, "bucket", bucket));
		} finally {
			size = readLocalMetadataFile(metadataFile);
		}
		return size == null ? null : size;
	}

	private void getRemoteFileIfNeeded(Bucket bucket, File metadataFile) {
		if (!metadataFile.exists() || readLocalMetadataFile(metadataFile) == null) {
			FileUtils.deleteQuietly(metadataFile);
			getRemoteFile(bucket, metadataFile);
		}
	}

	private Long readLocalMetadataFile(File metadataFile) {
		try {
			return flatFileStorage.readFlatFile(FileUtils
					.openInputStream(metadataFile));
		} catch (FileNotFoundException e) {
			return null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void getRemoteFile(Bucket bucket, File metadataFile) {
		String remotePathForMetadata = pathResolver.resolvePathForBucketMetadata(
				bucket, metadataFile);
		File metadataTransfersDir = localFileSystemPaths
				.getMetadataTransfersDirectory(bucket);
		Transaction getFileTransaction = GetFileTransaction.create(
				archiveFileSystem, remotePathForMetadata,
				metadataTransfersDir.getAbsolutePath(), metadataFile.getAbsolutePath());
		TransactionExecuter.executeTransaction(getFileTransaction);
	}

	/**
	 * @return a transaction for putting bucket size on the archiveFileSystem.
	 */
	public Transaction getBucketSizeTransaction(Bucket bucket) {
		File fileWithBucketSize = bucketSizeIO.getFileWithBucketSize(bucket);
		String temp = pathResolver.resolveTempPathForBucketMetadata(bucket,
				fileWithBucketSize);
		String bucketSizeFilePath = pathResolver.resolvePathForBucketMetadata(
				bucket, fileWithBucketSize);
		return PutFileTransaction.create(archiveFileSystem,
				fileWithBucketSize.getAbsolutePath(), temp, bucketSizeFilePath);
	}

	/**
	 * Instance with path resolver and archive file system.
	 * 
	 * @param localFileSystemPaths
	 */
	public static ArchiveBucketSize create(PathResolver pathResolver,
			ArchiveFileSystem archiveFileSystem, BucketSizeIO bucketSizeIO,
			LocalFileSystemPaths localFileSystemPaths) {
		return new ArchiveBucketSize(pathResolver, bucketSizeIO, archiveFileSystem,
				new FlatFileStorage(localFileSystemPaths), localFileSystemPaths);
	}
}
