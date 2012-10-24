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

import static java.util.Arrays.*;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;

/**
 * Cleans directories containing temporary data between archiver restarts. The
 * class does not do any magic to find out when the archiver starts up. Instead
 * the class is intended to be run when the archiver starts.
 */
public class StartUpCleaner {

	private final List<File> directoriesToClean;

	/**
	 * Constructor for giving a list of directories to clean. Use the factory
	 * method {@link StartUpCleaner#create()} for integration.
	 * 
	 * @param list
	 *          of directories to clean.
	 */
	protected StartUpCleaner(List<File> directoriesToClean) {
		this.directoriesToClean = directoriesToClean;
		for (File dir : directoriesToClean)
			if (!dir.isDirectory())
				throw new IllegalArgumentException();
	}

	/**
	 * Cleans given directories.
	 */
	public void clean() {
		for (File dir : directoriesToClean)
			for (File child : dir.listFiles())
				FileUtils.deleteQuietly(child);
	}

	/**
	 * @return integrated instance containing all the directories that should be
	 *         cleaned at start up.
	 */
	public static StartUpCleaner create() {
		LocalFileSystemPaths fsPaths = LocalFileSystemPaths.create();
		return new StartUpCleaner(asList(
				fsPaths.getThawLocksDirectoryForAllBuckets(),
				fsPaths.getThawTransfersDirectoryForAllBuckets()));
	}
}
