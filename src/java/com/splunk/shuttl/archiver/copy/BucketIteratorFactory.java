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
package com.splunk.shuttl.archiver.copy;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.model.BucketName;
import com.splunk.shuttl.archiver.model.LocalBucket;

public class BucketIteratorFactory {

	/**
	 * @return iterator over buckets in a directory.
	 */
	public Iterable<LocalBucket> iteratorInDirectory(File dir, String index) {
		ArrayList<LocalBucket> buckets = new ArrayList<LocalBucket>();
		for (File f : dir.listFiles(new BucketNameFileFilter()))
			if (f.isDirectory())
				buckets.add(createBucket(index, f));
		return buckets;
	}

	private LocalBucket createBucket(String index, File f) {
		return BucketFactory.createBucketWithIndexDirectoryAndFormat(index, f,
				BucketFormat.SPLUNK_BUCKET);
	}

	private static class BucketNameFileFilter implements FileFilter {

		@Override
		public boolean accept(File f) {
			try {
				verifyBucketName(f.getName());
				return true;
			} catch (RuntimeException e) {
				return false;
			}
		}

		private void verifyBucketName(String fileName) {
			BucketName bucketName = new BucketName(fileName);
			bucketName.getDB();
			bucketName.getEarliest();
			bucketName.getLatest();
			bucketName.getIndex();
		}
	}

}
