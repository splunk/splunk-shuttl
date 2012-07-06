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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;

import javax.management.InstanceNotFoundException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.server.mbeans.ShuttlArchiver;

/**
 * Constants for creating directories where the Archiver can store its locks,
 * unfinished buckets and other files.
 */
public class LocalFileSystemConstants {

	final String SAFE_BUCKETS_NAME = "safe-buckets";

	final String FAILED_BUCKETS_NAME = "failed-buckets";

	final String ARCHIVE_LOCKS_NAME = "archive-locks-dir";

	final String CSV_DIR_NAME = "csv-dir";

	final String THAW_LOCKS_NAME = "thaw-locks-dir";

	final String THAW_TRANSFERS_NAME = "thaw-transfers-dir";

	private final String archiverDirectoryPath;

	public LocalFileSystemConstants(String archiverDirectoryPath) {
		this.archiverDirectoryPath = archiverDirectoryPath;
	}

	/**
	 * Directory which contains all files created by the archiver.
	 */
	public File getArchiverDirectory() {
		String tildeAdjustedPath = archiverDirectoryPath.replace("~",
				FileUtils.getUserDirectoryPath());
		String pathWithoutTildeNorFileSchema = removeEventualFileSchema(tildeAdjustedPath);
		return new File(pathWithoutTildeNorFileSchema, "data");
	}

	private String removeEventualFileSchema(String path) {
		if (path.startsWith("file:/"))
			return path.replaceFirst("file:/", "");
		return path;
	}

	/**
	 * Safe location for the buckets to be archived. Stored away from Splunk,
	 * where Splunk cannot delete the buckets.
	 */
	public File getSafeDirectory() {
		return createDirectoryUnderArchiverDir(SAFE_BUCKETS_NAME);
	}

	private File createDirectoryUnderArchiverDir(String name) {
		File dir = new File(getArchiverDirectory(), name);
		dir.mkdirs();
		return dir;
	}

	/**
	 * Contains the failed bucket archiving transfers
	 */
	public File getFailDirectory() {
		return createDirectoryUnderArchiverDir(FAILED_BUCKETS_NAME);
	}

	/**
	 * Contains locks for archiving buckets.
	 */
	public File getArchiveLocksDirectory() {
		return createDirectoryUnderArchiverDir(ARCHIVE_LOCKS_NAME);
	}

	/**
	 * Contains CSV files when exporting buckets.
	 */
	public File getCsvDirectory() {
		return createDirectoryUnderArchiverDir(CSV_DIR_NAME);
	}

	/**
	 * Contains locks for thawing buckets.
	 */
	public File getThawLocksDirectory() {
		return createDirectoryUnderArchiverDir(THAW_LOCKS_NAME);
	}

	/**
	 * Temporary contains thaw transfers.
	 */
	public File getThawTransfersDirectory() {
		return createDirectoryUnderArchiverDir(THAW_TRANSFERS_NAME);
	}

	public static LocalFileSystemConstants create() {
		String archiverPath = getPathForArchiverData();
		return new LocalFileSystemConstants(archiverPath);
	}

	private static String getPathForArchiverData() {
		try {
			return ShuttlArchiver.getMBeanProxy().getLocalArchiverDir();
		} catch (InstanceNotFoundException e) {
			String archiverDirectoryPath = FileUtils.getTempDirectoryPath()
					+ File.separator + "ShuttlArchiverDataDirForTests";
			logAssumptionThatTestsAreBeingRun(e, archiverDirectoryPath);
			return archiverDirectoryPath;
		}
	}

	private static void logAssumptionThatTestsAreBeingRun(
			InstanceNotFoundException e, String archiverDirectoryPath) {
		Logger.getLogger(LocalFileSystemConstants.class).debug(
				warn("Tried getting local archiver directory from configuration.",
						"Configuration instance was not registered for ShuttlArchiver.",
						"Assuming tests are being run and returns path for archiver data: "
								+ archiverDirectoryPath, "archiver_directory_path",
						archiverDirectoryPath, "exception", e));
	}
}
