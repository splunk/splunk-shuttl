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
package com.splunk.shuttl.archiver.filesystem;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.BackendConfigurationFiles.ConfigurationFileDoesNotExist;
import com.splunk.shuttl.testutil.TUtilsEnvironment;

public class BackendConfigurationFilesTest {

	private BackendConfigurationFiles backendConfigurationFiles;
	private File configurationDir;

	@BeforeMethod(alwaysRun = true)
	public void setUp() {
		configurationDir = createDirectory();
		backendConfigurationFiles = new BackendConfigurationFiles(configurationDir);
	}

	@Test(groups = { "fast-unit" })
	public void getByNameFile_givenNamedFileExist_getsExistingFile() {
		File fooConf = createFileInParent(configurationDir, "foo.conf");
		File actualConf = backendConfigurationFiles.getByName(fooConf.getName());
		assertEquals(fooConf.getAbsolutePath(), actualConf.getAbsolutePath());
	}

	@Test(groups = { "fast-unit" }, expectedExceptions = { ConfigurationFileDoesNotExist.class })
	public void getByName_fileDoesNotExist_throws() {
		backendConfigurationFiles.getByName("doesNotExist");
	}

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "splunk.home" })
	public void create_withSplunkHome_getsHdfsPropertiesFile(
			final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {
			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				BackendConfigurationFiles bcf = BackendConfigurationFiles.create();
				File hdfs = bcf.getByName("hdfs.properties");
				assertTrue(hdfs.exists());
			}
		});
	}
}
