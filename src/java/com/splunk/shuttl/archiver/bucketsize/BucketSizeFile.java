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

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Logic for reading and writing a file with bucket size.
 */
public class BucketSizeFile {

	private static final Logger logger = Logger.getLogger(BucketSizeFile.class);

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
			return File.createTempFile(bucket.getName(), "size");
		} catch (IOException e) {
			logIOExceptionForCreatingFile(bucket, e);
			throw new RuntimeException(e);
		}
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

}
