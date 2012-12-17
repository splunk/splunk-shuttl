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
package com.splunk.shuttl.archiver.metastore;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.FailedToArchiveBucketException;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.filesystem.transaction.Transaction;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionException;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionExecuter;
import com.splunk.shuttl.archiver.filesystem.transaction.file.GetFileTransaction;
import com.splunk.shuttl.archiver.filesystem.transaction.file.PutFileTransaction;
import com.splunk.shuttl.archiver.metastore.FlatFileStorage.FlatFileReadException;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Puts and gets metadata that's unique for a bucket.
 */
public class MetadataStore {

	private static final Logger logger = Logger.getLogger(MetadataStore.class);

	private final PathResolver pathResolver;
	private final FlatFileStorage flatFileStorage;
	private final ArchiveFileSystem archiveFileSystem;
	private final TransactionExecuter transactionExecuter;
	private final LocalFileSystemPaths localFileSystemPaths;

	public MetadataStore(PathResolver pathResolver,
			FlatFileStorage flatFileStorage, ArchiveFileSystem archiveFileSystem,
			TransactionExecuter transactionExecuter,
			LocalFileSystemPaths localFileSystemPaths) {
		this.pathResolver = pathResolver;
		this.flatFileStorage = flatFileStorage;
		this.archiveFileSystem = archiveFileSystem;
		this.transactionExecuter = transactionExecuter;
		this.localFileSystemPaths = localFileSystemPaths;
	}

	/**
	 * Put metadata for a bucket with a filename as identifier.
	 */
	public void put(Bucket bucket, String fileName, String data) {
		try {
			transactionExecuter.execute(putBucketSizeTransaction(bucket, fileName,
					data));
		} catch (TransactionException e) {
			logger.error(did("Tried to transactionally transfer"
					+ " the bucketSize metadata to the archive.", e,
					"The transaction to complete.", "bucket", bucket));
			throw new FailedToArchiveBucketException(e);
		}
	}

	private Transaction putBucketSizeTransaction(Bucket bucket, String fileName,
			String data) {
		flatFileStorage.writeFlatFile(bucket, fileName, data);
		File fileWithBucketSize = flatFileStorage.getFlatFile(bucket, fileName);
		String temp = pathResolver.resolveTempPathForBucketMetadata(bucket,
				fileWithBucketSize);
		String bucketSizeFilePath = pathResolver.resolvePathForBucketMetadata(
				bucket, fileWithBucketSize);

		return PutFileTransaction.create(archiveFileSystem,
				fileWithBucketSize.getAbsolutePath(), temp, bucketSizeFilePath);
	}

	/**
	 * Read metadata stored with bucket and filename identifiers.
	 */
	public String read(Bucket bucket, String fileName) {
		File metadataFile = flatFileStorage.getFlatFile(bucket, fileName);
		getRemoteFileIfNeeded(bucket, metadataFile);

		String data = readLocalMetadataFile(metadataFile);
		if (data == null)
			throw new CouldNotReadMetadataException();
		else
			return data;
	}

	private void getRemoteFileIfNeeded(Bucket bucket, File metadataFile) {
		if (!metadataFile.exists() || readLocalMetadataFile(metadataFile) == null) {
			FileUtils.deleteQuietly(metadataFile);
			getRemoteFile(bucket, metadataFile);
		}
	}

	private String readLocalMetadataFile(File metadata) {
		try {
			return flatFileStorage.readFlatFile(metadata);
		} catch (FlatFileReadException e) {
			return null;
		}
	}

	private void getRemoteFile(Bucket bucket, File metadataFile) {
		String remotePathForMetadata = pathResolver.resolvePathForBucketMetadata(
				bucket, metadataFile);
		File metadataTransfersDir = localFileSystemPaths
				.getMetadataTransfersDirectory(bucket);

		Transaction getBucketSizeTransaction = GetFileTransaction.create(
				archiveFileSystem, remotePathForMetadata,
				metadataTransfersDir.getAbsolutePath(), metadataFile.getAbsolutePath());

		executeTransaction(bucket, metadataFile, getBucketSizeTransaction);
	}

	private void executeTransaction(Bucket bucket, File metadataFile,
			Transaction getBucketSizeTransaction) {
		try {
			transactionExecuter.execute(getBucketSizeTransaction);
		} catch (TransactionException e) {
			logger.warn(warn("Tried getting the remote metadata: " + metadataFile, e,
					"Will return null instead of the real metadata", "file",
					metadataFile, "bucket", bucket));
		}
	}

	public static class CouldNotReadMetadataException extends RuntimeException {

		private static final long serialVersionUID = 0;

	}

	public static MetadataStore create(ArchiveConfiguration config,
			ArchiveFileSystem archiveFileSystem,
			LocalFileSystemPaths localFileSystemPaths) {
		return new MetadataStore(new PathResolver(config), new FlatFileStorage(
				localFileSystemPaths), archiveFileSystem, new TransactionExecuter(),
				localFileSystemPaths);
	}
}
