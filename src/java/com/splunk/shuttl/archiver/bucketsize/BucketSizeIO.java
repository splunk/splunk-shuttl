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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Logic for reading and writing a file with bucket size.
 */
public class BucketSizeIO {

	private static final Logger logger = Logger.getLogger(BucketSizeIO.class);
	private final ArchiveFileSystem archiveFileSystem;
	private LocalFileSystemPaths localFileSystemPaths;

	/**
	 * @param archiveFileSystem
	 *          to read the file size off of.
	 * @param localFileSystemPaths
	 *          for storing bucket size on local disk.
	 */
	public BucketSizeIO(ArchiveFileSystem archiveFileSystem,
			LocalFileSystemPaths localFileSystemPaths) {
		this.archiveFileSystem = archiveFileSystem;
		this.localFileSystemPaths = localFileSystemPaths;
	}

	/**
	 * @return a file that contains information about the specified bucket's size.
	 */
	public File getFileWithBucketSize(Bucket bucket) {
		File tempFile = getTempFileForBucket(bucket);
		writeSizeToFile(bucket.getSize(), tempFile);
		return tempFile;
	}

	private File getTempFileForBucket(Bucket bucket) {
		try {
			return createFile(bucket);
		} catch (IOException e) {
			logIOExceptionForCreatingFile(bucket, e);
			throw new RuntimeException(e);
		}
	}

	private File createFile(Bucket bucket) throws IOException {
		File file = new File(localFileSystemPaths.getMetadataDirectory(),
				bucket.getName() + ".size");
		file.createNewFile();
		return file;
	}

	private void logIOExceptionForCreatingFile(Bucket bucket, IOException e) {
		logger.debug(did("Tried creating temp file for BucketSizeFile.", e,
				"To create temp file.", "bucket", bucket, "exception", e));
	}

	private void writeSizeToFile(Long size, File file) {
		String content = size + "";
		try {
			FileUtils.write(file, content);
		} catch (IOException e) {
			logIOExceptionForWritingToFile(file, content, e);
			throw new RuntimeException(e);
		}
	}

	private void logIOExceptionForWritingToFile(File file, String content,
			IOException e) {
		logger.debug(did("Tried to write to file for BucketSizeFile.", e,
				"To write size to file", "file", file, "content", content));
	}

	/**
	 * @param uriToFileWithBucketSize
	 * @return
	 */
	public long readSizeFromRemoteFile(URI uriToFileWithBucketSize) {
		InputStream inputStream = getInputStreamToFile(uriToFileWithBucketSize);
		List<String> lines = getLinesFromInputStream(inputStream);
		return Long.parseLong(lines.get(0));
	}

	private InputStream getInputStreamToFile(URI uriToFileWithBucketSize) {
		try {
			return archiveFileSystem.openFile(uriToFileWithBucketSize);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private List<String> getLinesFromInputStream(InputStream inputStream) {
		try {
			return IOUtils.readLines(inputStream);
		} catch (IOException e) {
			throw new RuntimeException(e); // TODO: Test this, so it's documented that
																			// this is expected
			// behaviour.
		}
	}

}
