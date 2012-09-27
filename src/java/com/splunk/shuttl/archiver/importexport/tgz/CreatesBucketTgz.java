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

import com.splunk.shuttl.archiver.importexport.GetsBucketsExportFile;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Creates a tgz file from a bucket. Useful when either compressing the buckets
 * or when wanting to represent a bucket as a single file.
 */
public class CreatesBucketTgz {

	public static class TgzBucketCreationFailedException extends RuntimeException {

		private static final long serialVersionUID = 1L;

	}

	private final ShellExecutor shellExecutor;
	private GetsBucketsExportFile getsBucketsExportFile;

	public CreatesBucketTgz(ShellExecutor shellExecutor,
			GetsBucketsExportFile getsBucketsExportFile) {
		this.shellExecutor = shellExecutor;
		this.getsBucketsExportFile = getsBucketsExportFile;
	}

	/**
	 * Creates a .tgz file from a bucket.
	 */
	public File createTgz(Bucket bucket) {
		File tar = getsBucketsExportFile.getExportFile(bucket, "tar");
		File tgz = getsBucketsExportFile.getExportFile(bucket, "tgz");
		try {
			createTgzFileFromBucket(bucket, tar, tgz);
		} catch (TgzBucketCreationFailedException e) {
			tgz.delete();
			throw e;
		} finally {
			tar.delete();
		}
		return tgz;
	}

	private void createTgzFileFromBucket(Bucket bucket, File tar, File tgz) {
		String[] command = buildTgzCommand(bucket, tar, tgz);
		executeCommand(command);
	}

	private String[] buildTgzCommand(Bucket bucket, File tar, File tgz) {
		String tarCmd = "tar -c " + bucket.getDirectory().getAbsolutePath() + " > "
				+ tar.getAbsolutePath();
		String gzipCmd = "gzip -c " + tar + " > " + tgz;
		String[] command = { "/bin/sh", "-c", tarCmd + " && " + gzipCmd };
		return command;
	}

	private void executeCommand(String[] cmd) {
		HashMap<String, String> emptyMap = new HashMap<String, String>();
		int exit = shellExecutor.executeCommand(emptyMap, asList(cmd));
		if (exit != 0)
			throw new TgzBucketCreationFailedException();
	}

	public static CreatesBucketTgz create(File tgzDir) {
		return new CreatesBucketTgz(ShellExecutor.getInstance(),
				new GetsBucketsExportFile(tgzDir));
	}
}
