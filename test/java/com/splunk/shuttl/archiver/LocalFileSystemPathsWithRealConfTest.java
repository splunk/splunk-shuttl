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

import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.testutil.TUtilsMBean;

@Test(groups = { "end-to-end" })
public class LocalFileSystemPathsWithRealConfTest {

	private File archiverDirWithMBeanConf;

	@BeforeMethod
	@Parameters(value = { "shuttl.conf.dir" })
	public void setUp(String shuttlConfsDirPath) {
		File confsDir = new File(shuttlConfsDirPath);
		TUtilsMBean.runWithRegisteredMBeans(confsDir, new Runnable() {
			@Override
			public void run() {
				archiverDirWithMBeanConf = LocalFileSystemPaths.create()
						.getArchiverDirectory();
				FileUtils.deleteQuietly(archiverDirWithMBeanConf);
			}
		});
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(archiverDirWithMBeanConf);
	}

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "shuttl.conf.dir" })
	public void create_withMBeanRegistered_archiverDirectoryIsCreatable(
			String shuttlConfsDirPath) throws IOException {
		File confsDir = new File(shuttlConfsDirPath);
		TUtilsMBean.runWithRegisteredMBeans(confsDir, new Runnable() {

			@Override
			public void run() {
				archiverDirWithMBeanConf = LocalFileSystemPaths.create()
						.getArchiverDirectory();
				assertFalse(archiverDirWithMBeanConf.exists());
				assertTrue(archiverDirWithMBeanConf.mkdirs());
				assertTrue(archiverDirWithMBeanConf.exists());
			}
		});
	}

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "shuttl.conf.dir" })
	public void create_withMBeanRegistered_creatingWithConfigOrMBeanAreEqual(
			String shuttlConfsDirPath) throws IOException {
		File confsDir = new File(shuttlConfsDirPath);
		TUtilsMBean.runWithRegisteredMBeans(confsDir, new Runnable() {

			@Override
			public void run() {
				LocalFileSystemPaths create = LocalFileSystemPaths.create();
				LocalFileSystemPaths createWithConfig = LocalFileSystemPaths
						.create(ArchiveConfiguration.getSharedInstance());
				archiverDirWithMBeanConf = create.getArchiverDirectory();
				assertEquals(create.getArchiverDirectory().getAbsolutePath(),
						createWithConfig.getArchiverDirectory().getAbsolutePath());
			}
		});
	}

	@Test(expectedExceptions = { ArchiverMBeanNotRegisteredException.class })
	public void create_withNoArchiverMBeanRegistration_throwsArchiverMBeanNotRegisteredException() {
		LocalFileSystemPaths.create();
	}
}
