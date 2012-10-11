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
package com.splunk.shuttl.archiver.model;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketFormat;

/**
 * Creates {@link Bucket}s.<br/>
 * For example: Needed for tests when creating buckets and expecting failures.
 * Cannot mock static factory methods.
 */
public class BucketFactory {

	private static final Logger logger = Logger.getLogger(BucketFactory.class);

	/**
	 * Instance method for
	 * {@link BucketFactory#createWithIndexAndDirectory(String, File)}, which
	 * makes it mockable and testable.
	 */
	public LocalBucket createWithIndexDirectoryAndFormat(String index,
			File bucketFile, BucketFormat format) {
		return BucketFactory.createBucketWithIndexDirectoryAndFormat(index,
				bucketFile, format);
	}

	/**
	 * @return Bucket instance with specified index and bucket. Suitable for
	 *         creating bucket with {@link BucketFormat#SPLUNK_BUCKET}, since the
	 *         format can be decided from the directory.
	 */
	public static LocalBucket createBucketWithIndexDirectoryAndFormat(
			String index, File bucketFile, BucketFormat format) {
		try {
			return new LocalBucket(bucketFile, index, format);
		} catch (FileNotFoundException e) {
			logFileNotFoundException(bucketFile, e);
			throw new RuntimeException(e);
		} catch (FileNotDirectoryException e) {
			logFileNotDirectoryException(bucketFile, e);
			throw new RuntimeException(e);
		}
	}

	private static void logFileNotFoundException(File bucketFile,
			FileNotFoundException e) {
		logger.debug(did("Created bucket from file", "Got FileNotFoundException",
				"To create bucket from file", "file", bucketFile, "exception", e));
	}

	private static void logFileNotDirectoryException(File bucketFile,
			FileNotDirectoryException e) {
		logger.debug(did("Created bucket from file",
				"Got FileNotDirectoryException", "To create bucket from file", "file",
				bucketFile, "exception", e));
	}

	/**
	 * Instance method for
	 * {@link BucketFactory#createBucketWithIndexDirectoryAndSize}, which makes it
	 * mockable and testable.
	 */
	public LocalBucket createWithIndexDirectoryAndSize(String index,
			File bucketFile, BucketFormat format, Long size) {
		return createBucketWithIndexDirectoryAndSize(index, bucketFile, format,
				size);
	}

	/**
	 * Creates a bucket with index, directory and size.
	 */
	public static LocalBucket createBucketWithIndexDirectoryAndSize(String index,
			File bucketFile, BucketFormat format, Long size) {
		return createBucketWithIndexDirectoryBucketNameAndSize(index, bucketFile,
				bucketFile.getAbsolutePath(), format, size);
	}

	/**
	 * Creates a bucket with index, directory and size.
	 */
	public static LocalBucket createBucketWithIndexDirectoryBucketNameAndSize(
			String index, File bucketFile, String bucketName, BucketFormat format,
			Long size) {
		try {
			return new LocalBucket(bucketFile, index, bucketName, format, size);
		} catch (FileNotFoundException e) {
			logFileNotFoundException(bucketFile, e);
			throw new RuntimeException(e);
		} catch (FileNotDirectoryException e) {
			logFileNotDirectoryException(bucketFile, e);
			throw new RuntimeException(e);
		}
	}

}
