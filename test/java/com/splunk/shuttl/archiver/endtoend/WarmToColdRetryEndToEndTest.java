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
package com.splunk.shuttl.archiver.endtoend;

import static java.util.Arrays.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.HashMap;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.thaw.SplunkIndexedLayerFactory;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEndToEnd;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "end-to-end" })
public class WarmToColdRetryEndToEndTest {

	@Parameters(value = { "shuttl.conf.dir", "splunk.home" })
	public void _bucketInColdDirectoryWithNoCopyReceipt_archivesBucket(
			final String shuttlConfDirPath, final String relativeSplunkHome) {
		final File shuttlConfDir = new File(shuttlConfDirPath);
		final String splunkHome = new File(relativeSplunkHome).getAbsolutePath();
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				try {
					doRunTest(shuttlConfDir, splunkHome);
				} finally {
					TUtilsEndToEnd.cleanHadoopFileSystem(shuttlConfDir, splunkHome);
				}
			}

			private void doRunTest(final File shuttlConfDir, final String splunkHome) {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				TUtilsMBean.runWithRegisteredMBeans(shuttlConfDir, new Runnable() {

					@Override
					public void run() {
						LocalBucket bucket = putBucketInColdDirectory();
						executeWarmToColdRetryScript(splunkHome);
						assertBucketWasArchived(bucket);
					}
				});
			}

		});
	}

	private LocalBucket putBucketInColdDirectory() {
		return TUtilsBucket.createBucketInDirectoryWithIndex(getColdDirectory(),
				TUtilsEndToEnd.REAL_SPLUNK_INDEX);
	}

	private File getColdDirectory() {
		return new File(SplunkIndexedLayerFactory.getLoggedInSplunkService()
				.getIndexes().get(TUtilsEndToEnd.REAL_SPLUNK_INDEX)
				.getColdPathExpanded());
	}

	private void executeWarmToColdRetryScript(String splunkHome) {
		HashMap<String, String> env = new HashMap<String, String>();
		env.put("SPLUNK_HOME", splunkHome);
		ShellExecutor shellExecutor = ShellExecutor.getInstance();
		int exit = shellExecutor.executeCommand(env,
				asList(getWarmToColdRetryScript(splunkHome).getAbsolutePath()));
		assertEquals(exit, 0);
	}

	private File getWarmToColdRetryScript(String splunkHome) {
		File warmToColdRetryScript = new File(new File(splunkHome),
				"etc/apps/shuttl/bin/warmToColdRetry.sh");
		assertTrue(warmToColdRetryScript.exists());
		assertTrue(warmToColdRetryScript.canExecute());
		return warmToColdRetryScript;
	}

	private void assertBucketWasArchived(LocalBucket bucket) {
		ArchiveConfiguration config = ArchiveConfiguration
				.createConfigurationFromMBean();
		PathResolver pathResolver = new PathResolver(config);
		ArchiveFileSystem archive = ArchiveFileSystemFactory
				.getWithConfiguration(config);
		try {
			assertTrue(archive.exists(pathResolver.resolveArchivePath(bucket)));
		} catch (Exception e) {
			TUtilsTestNG.failForException(null, e);
		}
	}
}
