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
package com.splunk.shuttl.archiver.importexport.tgz;

import static java.util.Arrays.*;

import java.io.File;
import java.util.HashMap;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.util.UtilsBucket;

/**
 * Imports Tgz buckets to {@link BucketFormat#SPLUNK_BUCKET}
 */
public class TgzImporter implements BucketImporter {

	private ShellExecutor shellExecutor;

	/**
	 * @param instance
	 */
	public TgzImporter(ShellExecutor shellExecutor) {
		this.shellExecutor = shellExecutor;
	}

	@Override
	public Bucket importBucket(Bucket bucket) {
		File tgzFile = UtilsBucket.getTgzFile(bucket);
		int exit = executeCommand(buildCommand(bucket, tgzFile));
		deleteOnSuccess(bucket, tgzFile, exit);
		return BucketFactory.createBucketWithIndexDirectoryAndFormat(
				bucket.getIndex(), bucket.getDirectory(), BucketFormat.SPLUNK_BUCKET);
	}

	private String[] buildCommand(Bucket b, File tgzFile) {
		String tgzPath = tgzFile.getAbsolutePath();
		String bucketParentPath = b.getDirectory().getParentFile()
				.getAbsolutePath();
		String extractCmd = "tar -xf " + tgzPath + " -C " + bucketParentPath;
		return new String[] { "/bin/sh", "-c", extractCmd };
	}

	private int executeCommand(String[] command) {
		int exit = shellExecutor.executeCommand(new HashMap<String, String>(),
				asList(command));
		return exit;
	}

	private void deleteOnSuccess(Bucket bucket, File tgzFile, int exit) {
		if (exit == 0)
			tgzFile.delete();
		else
			throw new TgzImportFailedException("Failed to import bucket: " + bucket);
	}

	public static class TgzImportFailedException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		public TgzImportFailedException(String message) {
			super(message);
		}

	}
}
