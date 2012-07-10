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
import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.TUtilsMBean;

@Test(groups = { "slow-unit" })
public class LocalFileSystemConstantsWithRealConfTest {

	private File archiverDirWithMBeanConf;

	@BeforeMethod
	public void setUp() {
		TUtilsMBean.runWithRegisteredShuttlArchiverMBean(new Runnable() {
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
		TUtilsMBean.unregisterShuttlArchiverMBean();
		FileUtils.deleteQuietly(archiverDirWithMBeanConf);
	}

	@Test(groups = { "slow-unit" })
	public void create_withMBeanRegistered_archiverDirectoryIsCreatable()
			throws IOException {
		TUtilsMBean.registerShuttlArchiverMBean();
		archiverDirWithMBeanConf = LocalFileSystemPaths.create()
				.getArchiverDirectory();
		assertFalse(archiverDirWithMBeanConf.exists());
		assertTrue(archiverDirWithMBeanConf.mkdirs());
		assertTrue(archiverDirWithMBeanConf.exists());
	}

	@Test(expectedExceptions = { ArchiverMBeanNotRegisteredException.class })
	public void create_withNoArchiverMBeanRegistration_throwsArchiverMBeanNotRegisteredException() {
		LocalFileSystemPaths.create();
	}
}
