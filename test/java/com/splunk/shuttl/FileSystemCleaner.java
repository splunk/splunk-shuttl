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
package com.splunk.shuttl;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterTest;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.testutil.TUtilsFile;
import com.splunk.shuttl.testutil.TUtilsMBean;

/**
 * Cleans the file system from created files.
 */
public class FileSystemCleaner {

	@AfterTest(alwaysRun = true)
	public void cleanFileSystemFromCreatedFilesDuringTests() {
		cleanShuttlTestDirectory();
		cleanConfiguredLocalFileSystemPaths();
	}

	public void cleanShuttlTestDirectory() {
		File shuttlTestDirectory = TUtilsFile.getShuttlTestDirectory();
		File[] files = shuttlTestDirectory.listFiles();
		if (files != null)
			for (File file : files)
				FileUtils.deleteQuietly(file);
	}

	public void cleanConfiguredLocalFileSystemPaths() {
		TUtilsMBean.runWithRegisteredMBeans(new Runnable() {
			@Override
			public void run() {
				FileUtils.deleteQuietly(LocalFileSystemPaths.create()
						.getArchiverDirectory());
			}
		});
	}

}
