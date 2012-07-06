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
package com.splunk.shuttl.archiver;

import java.io.File;

import org.apache.commons.io.FileUtils;

/**
 * Constants for creating directories where the Archiver can store its locks,
 * unfinished buckets and other files.
 */
public class LocalFileSystemConstants {

	final String ARCHIVER_DIRECTORY_PATH = FileUtils.getUserDirectoryPath()
			+ File.separator + "SplunkArchiverFiles";

	final String SAFE_PATH = ARCHIVER_DIRECTORY_PATH + File.separator
			+ "safe-buckets";

	final String FAIL_PATH = ARCHIVER_DIRECTORY_PATH + File.separator
			+ "failed-buckets";

	final String ARCHIVE_LOCKS_PATH = ARCHIVER_DIRECTORY_PATH + File.separator
			+ "archive-locks-dir";

	final String CSV_PATH = ARCHIVER_DIRECTORY_PATH + File.separator + "csv-dir";

	final String THAW_LOCKS_PATH = ARCHIVER_DIRECTORY_PATH + File.separator
			+ "thaw-locks-dir";

	final String THAW_TRANSFERS_PATH = ARCHIVER_DIRECTORY_PATH + File.separator
			+ "thaw-transfers-dir";

	/**
	 * Directory which contains all files created by the archiver.
	 */
	public File getArchiverDirectory() {
		return createDirectory(ARCHIVER_DIRECTORY_PATH);
	}

	private File createDirectory(String path) {
		File dir = new File(path);
		dir.mkdirs();
		return dir;
	}

	/**
	 * Safe location for the buckets to be archived. Stored away from Splunk,
	 * where Splunk cannot delete the buckets.
	 */
	public File getSafeDirectory() {
		return createDirectory(SAFE_PATH);
	}

	/**
	 * Contains the failed bucket archiving transfers
	 */
	public File getFailDirectory() {
		return createDirectory(FAIL_PATH);
	}

	/**
	 * Contains locks for archiving buckets.
	 */
	public File getArchiveLocksDirectory() {
		return createDirectory(ARCHIVE_LOCKS_PATH);
	}

	/**
	 * Contains CSV files when exporting buckets.
	 */
	public File getCsvDirectory() {
		return createDirectory(CSV_PATH);
	}

	/**
	 * Contains locks for thawing buckets.
	 */
	public File getThawLocksDirectory() {
		return createDirectory(THAW_LOCKS_PATH);
	}

	/**
	 * Temporary contains thaw transfers.
	 */
	public File getThawTransfersDirectory() {
		return createDirectory(THAW_TRANSFERS_PATH);
	}

}
