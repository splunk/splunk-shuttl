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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.bucketsize.ArchiveBucketSize;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.fileSystem.FileOverwriteException;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Class for transferring buckets
 */
public class ArchiveBucketTransferer {

	private final ArchiveFileSystem archiveFileSystem;
	private final static Logger logger = Logger
			.getLogger(ArchiveBucketTransferer.class);
	private final PathResolver pathResolver;
	private final ArchiveBucketSize archiveBucketSize;

	public ArchiveBucketTransferer(ArchiveFileSystem archive,
			PathResolver pathResolver, ArchiveBucketSize archiveBucketSize) {
		this.archiveFileSystem = archive;
		this.pathResolver = pathResolver;
		this.archiveBucketSize = archiveBucketSize;
	}

	/**
	 * Transfers the bucket and its content to the archive.
	 * 
	 * @param bucket
	 *          to transfer to {@link ArchiveFileSystem}
	 */
	public void transferBucketToArchive(Bucket bucket) {
		URI destination = pathResolver.resolveArchivePath(bucket);
		logger.info(will("attempting to transfer bucket to archive", "bucket",
				bucket, "destination", destination));
		try {
			archiveFileSystem.putFileAtomically(bucket.getDirectory(), destination);
			archiveBucketSize.putSize(bucket);
		} catch (FileNotFoundException e) {
			logFileNotFoundException(bucket, destination, e);
			throw new RuntimeException(e);
		} catch (FileOverwriteException e) {
			logFileOverwriteException(bucket, destination, e);
			throw new RuntimeException(e);
		} catch (IOException e) {
			logIOException(bucket, destination, e);
			throw new RuntimeException(e);
		}
	}

	private void logFileNotFoundException(Bucket bucket, URI destination,
			FileNotFoundException e) {
		logger.error(did("attempted to transfer bucket to archive",
				"bucket path does not exist", "success", "bucket", bucket,
				"destination", destination, "exception", e));
	}

	private void logFileOverwriteException(Bucket bucket, URI destination,
			FileOverwriteException e) {
		logger
				.error(did("attempted to transfer bucket to archive",
						"a bucket with the same path already exists on the filesystem",
						"success", "bucket", bucket, "destination", destination,
						"exception", e));
	}

	private void logIOException(Bucket bucket, URI destination, IOException e) {
		logger.error(did("attempted to transfer bucket to archive",
				"IOException raised", "success", "bucket", bucket, "destination",
				destination, "exception", e));
	}

	/**
	 * Instance of {@link ArchiveBucketTransferer} with file system and
	 * configuration.
	 */
	public static ArchiveBucketTransferer create(
			ArchiveFileSystem archiveFileSystem, ArchiveConfiguration config) {
		PathResolver pathResolver = new PathResolver(config);
		return new ArchiveBucketTransferer(archiveFileSystem, pathResolver,
				ArchiveBucketSize.create(pathResolver, archiveFileSystem));
	}
}
