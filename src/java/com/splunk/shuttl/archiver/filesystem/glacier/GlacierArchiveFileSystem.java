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
package com.splunk.shuttl.archiver.filesystem.glacier;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketDeleter;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.transaction.bucket.BucketTransactionCleaner;
import com.splunk.shuttl.archiver.filesystem.transaction.bucket.TransfersBuckets;
import com.splunk.shuttl.archiver.filesystem.transaction.file.FileTransactionCleaner;
import com.splunk.shuttl.archiver.filesystem.transaction.file.TransfersFiles;
import com.splunk.shuttl.archiver.importexport.tgz.TgzFormatExporter;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * The glacier file system is not good for storing multiple files, it will
 * therefore rely on s3 to handle the storing of meta data and file structure.
 * It supports only buckets that contain a single file.
 */
public class GlacierArchiveFileSystem implements ArchiveFileSystem {

	private final ArchiveFileSystem hadoop;
	private final GlacierClient glacierClient;
	private final TgzFormatExporter tgzFormatExporter;
	private final Logger logger;
	private BucketDeleter bucketDeleter;

	public GlacierArchiveFileSystem(ArchiveFileSystem hadoop,
			GlacierClient glacierClient, TgzFormatExporter tgzFormatExporter,
			Logger logger, BucketDeleter bucketDeleter) {
		this.hadoop = hadoop;
		this.glacierClient = glacierClient;
		this.tgzFormatExporter = tgzFormatExporter;
		this.logger = logger;
		this.bucketDeleter = bucketDeleter;
	}

	private void putBucket(LocalBucket bucket, String temp, String dst)
			throws IOException {
		if (bucket.getFormat().equals(BucketFormat.SPLUNK_BUCKET)) {
			LocalBucket tgzBucket = exportToTgzBucketWithWarning(bucket);
			uploadBucket(tgzBucket, dst);
			bucketDeleter.deleteBucket(tgzBucket);
		} else {
			uploadBucket(bucket, dst);
		}
	}

	private LocalBucket exportToTgzBucketWithWarning(LocalBucket localBucket) {
		LocalBucket bucketToUpload = tgzFormatExporter.exportBucket(localBucket);
		logger.warn(warn("Exported bucket to tgz because glacier should only "
				+ "upload one file", "Bucket got exported",
				"Will upload this tgz bucket. You can prevent this "
						+ "warning by configuring glacier with bucket formats "
						+ "that already are one file, i.e. CSV and SPLUNK_BUCKET_TGZ",
				"bucket", localBucket));
		return bucketToUpload;
	}

	private void uploadBucket(LocalBucket bucketToUpload, String dst) {
		File[] bucketFiles = bucketToUpload.getDirectory().listFiles();
		if (bucketFiles.length != 1)
			throw new GlacierArchivingException("Bucket has to be "
					+ "represented with only one file. Bucket: " + bucketToUpload);

		File bucketFile = bucketFiles[0];
		try {
			glacierClient.upload(bucketFile, dst);
		} catch (Exception e) {
			throw new GlacierArchivingException("Got exception when uploading "
					+ "file to glacier. File: " + bucketFile + ", exception: " + e);
		}
	}

	private void getBucket(Bucket remoteBucket, File temp, File dst)
			throws IOException {
		String path = remoteBucket.getPath();
		try {
			glacierClient.downloadToDir(path, temp);
		} catch (Exception e) {
			throw new GlacierThawingException("Got exception when downloading "
					+ "from glacier. Path: " + path);
		}
	}

	@Override
	public void mkdirs(String path) throws IOException {
		hadoop.mkdirs(path);
	}

	@Override
	public void rename(String from, String to) throws IOException {
		hadoop.rename(from, to);
	}

	@Override
	public List<String> listPath(String pathToBeListed) throws IOException {
		return hadoop.listPath(pathToBeListed);
	}

	@Override
	public InputStream openFile(String fileOnArchiveFileSystem)
			throws IOException {
		return hadoop.openFile(fileOnArchiveFileSystem);
	}

	@Override
	public TransfersBuckets getBucketTransferer() {
		return new TransfersBuckets() {

			@Override
			public void put(Bucket localBucket, String temp, String dst)
					throws IOException {
				putBucket((LocalBucket) localBucket, temp, dst);
			}

			@Override
			public void get(Bucket remoteBucket, File temp, File dst)
					throws IOException {
				getBucket(remoteBucket, temp, dst);
			}
		};
	}

	@Override
	public TransfersFiles getFileTransferer() {
		return hadoop.getFileTransferer();
	}

	@Override
	public BucketTransactionCleaner getBucketTransactionCleaner() {
		return new BucketTransactionCleaner() {

			@Override
			public void cleanTransaction(Bucket src, String temp) {
				// Do nothing.
			}
		};
	}

	@Override
	public FileTransactionCleaner getFileTransactionCleaner() {
		return new FileTransactionCleaner() {

			@Override
			public void cleanTransaction(String src, String temp) {
				hadoop.getFileTransactionCleaner().cleanTransaction(src, temp);
			}
		};
	}
}
