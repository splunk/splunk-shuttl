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
import java.net.URI;
import java.util.List;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketDeleter;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.hadoop.HadoopArchiveFileSystem;
import com.splunk.shuttl.archiver.importexport.tgz.TgzFormatExporter;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * The glacier file system is not good for storing multiple files, it will
 * therefore rely on s3 to handle the storing of meta data and file structure.
 * It supports only buckets that contain a single file.
 */
public class GlacierArchiveFileSystem implements ArchiveFileSystem {

	private final HadoopArchiveFileSystem hadoop;
	private final GlacierClient glacierClient;
	private final TgzFormatExporter tgzFormatExporter;
	private final Logger logger;
	private BucketDeleter bucketDeleter;
	private final String id;
	private final String secret;
	private final String endpoint;
	private final String vault;

	public GlacierArchiveFileSystem(HadoopArchiveFileSystem hadoop,
			GlacierClient glacierClient, TgzFormatExporter tgzFormatExporter,
			Logger logger, BucketDeleter bucketDeleter, String id, String secret,
			String endpoint, String vault) {
		this.hadoop = hadoop;
		this.glacierClient = glacierClient;
		this.tgzFormatExporter = tgzFormatExporter;
		this.logger = logger;
		this.bucketDeleter = bucketDeleter;
		this.id = id;
		this.secret = secret;
		this.endpoint = endpoint;
		this.vault = vault;
	}

	@Override
	public void putBucket(Bucket bucket, URI temp, URI dst) throws IOException {
		if (bucket.getFormat().equals(BucketFormat.SPLUNK_BUCKET)) {
			Bucket tgzBucket = exportToTgzBucketWithWarning(bucket);
			uploadBucket(tgzBucket, dst);
			bucketDeleter.deleteBucket(tgzBucket);
		} else {
			uploadBucket(bucket, dst);
		}
	}

	private Bucket exportToTgzBucketWithWarning(Bucket localBucket) {
		Bucket bucketToUpload = tgzFormatExporter.exportBucket(localBucket);
		logger.warn(warn("Exported bucket to tgz because glacier should only "
				+ "upload one file", "Bucket got exported",
				"Will upload this tgz bucket. You can prevent this "
						+ "warning by configuring glacier with bucket formats "
						+ "that already are one file, i.e. CSV and SPLUNK_BUCKET_TGZ",
				"bucket", localBucket));
		return bucketToUpload;
	}

	private void uploadBucket(Bucket bucketToUpload, URI dst) {
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

	@Override
	public void getBucket(Bucket remoteBucket, File temp, File dst)
			throws IOException {
		URI uri = remoteBucket.getURI();
		try {
			glacierClient.downloadToDir(uri, temp);
		} catch (Exception e) {
			throw new GlacierThawingException("Got exception when downloading "
					+ "from glacier. URI: " + uri);
		}
	}

	@Override
	public void cleanBucketTransaction(Bucket bucket, URI temp) {
		// Do nothing.
	}

	public String getId() {
		return id;
	}

	public String getSecret() {
		return secret;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public String getVault() {
		return vault;
	}

	@Override
	public void putFile(File src, URI temp, URI dst) throws IOException {
		hadoop.putFile(src, temp, dst);
	}

	@Override
	public void getFile(URI src, File temp, File dst) throws IOException {
		hadoop.getFile(src, temp, dst);
	}

	@Override
	public void mkdirs(URI uri) throws IOException {
		hadoop.mkdirs(uri);
	}

	@Override
	public void rename(URI from, URI to) throws IOException {
		hadoop.rename(from, to);
	}

	@Override
	public void cleanFileTransaction(URI src, URI temp) {
		hadoop.cleanFileTransaction(src, temp);
	}

	@Override
	public List<URI> listPath(URI pathToBeListed) throws IOException {
		return hadoop.listPath(pathToBeListed);
	}

	@Override
	public InputStream openFile(URI fileOnArchiveFileSystem) throws IOException {
		return hadoop.openFile(fileOnArchiveFileSystem);
	}

}
