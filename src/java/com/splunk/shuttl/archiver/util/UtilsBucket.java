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
package com.splunk.shuttl.archiver.util;

import java.io.File;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.NoFileFoundException;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Util functions for {@link Bucket}s.
 */
public class UtilsBucket {

	/**
	 * @return the csv {@link File} representing the bucket
	 * @throws {@link NoFileFoundException} when no csv file was found.
	 */
	public static File getCsvFile(LocalBucket csvBucket) {
		return getFileFromBucket(csvBucket, BucketFormat.CSV);
	}

	private static boolean isBucketEmpty(LocalBucket csvBucket) {
		return csvBucket.getDirectory().listFiles().length == 0;
	}

	public static File getFileFromBucket(LocalBucket bucket, BucketFormat format) {
		if (isBucketEmpty(bucket))
			throw new IllegalArgumentException("Bucket was empty!");
		else
			return doGetFileFromBucket(bucket, format);
	}

	private static File doGetFileFromBucket(LocalBucket bucket,
			BucketFormat format) {
		String extensionWithoutDot = BucketFormat.extensionOfFormat(format)
				.replaceFirst(".", "");
		for (File file : bucket.getDirectory().listFiles())
			if (file.getName().endsWith(extensionWithoutDot))
				return file;
		throw new NoFileFoundException();
	}

	/**
	 * @return the .tgz file in a bucket, which has a TGZ bucket format.
	 */
	public static File getTgzFile(LocalBucket realTgzBucket) {
		return getFileFromBucket(realTgzBucket, BucketFormat.SPLUNK_BUCKET_TGZ);
	}
}
