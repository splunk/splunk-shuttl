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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Logic for reading and writing a file with bucket size.
 */
public class BucketSizeIO {

	/**
	 * For compatibility with older Shuttl versions.
	 */
	private static final String FILE_NAME = PathResolver.BUCKET_SIZE_FILE_NAME;
	private final ArchiveFileSystem archiveFileSystem;
	private final FlatFileStorage flatFileStorage;

	/**
	 * @param archiveFileSystem
	 *          to read the file size off of.
	 * @param localFileSystemPaths
	 *          for storing bucket size on local disk.
	 */
	public BucketSizeIO(ArchiveFileSystem archiveFileSystem,
			FlatFileStorage flatFileStorage) {
		this.archiveFileSystem = archiveFileSystem;
		this.flatFileStorage = flatFileStorage;
	}

	/**
	 * @return a file that contains information about the specified bucket's size.
	 */
	public File getFileWithBucketSize(Bucket bucket) {
		flatFileStorage.writeFlatFile(bucket, FILE_NAME, bucket.getSize());
		return flatFileStorage.getFlatFile(bucket, FILE_NAME);
	}

	/**
	 * @param filePathForSizeFile
	 * @return
	 */
	public long readSizeFromRemoteFile(String filePathForSizeFile) {
		InputStream inputStream = getInputStreamToFile(filePathForSizeFile);
		return flatFileStorage.readFlatFile(inputStream);
	}

	private InputStream getInputStreamToFile(String pathToFileWithBucketSize) {
		try {
			return archiveFileSystem.openFile(pathToFileWithBucketSize);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static BucketSizeIO create(ArchiveFileSystem archiveFileSystem,
			LocalFileSystemPaths localFileSystemPaths) {
		return new BucketSizeIO(archiveFileSystem, new FlatFileStorage(
				localFileSystemPaths));
	}
}
