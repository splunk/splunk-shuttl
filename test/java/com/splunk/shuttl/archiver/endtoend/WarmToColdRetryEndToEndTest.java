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
import java.io.IOException;
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

@Test(groups = { "end-to-end" }, enabled = false)
public class WarmToColdRetryEndToEndTest {

	@Parameters(value = { "shuttl.conf.dir", "splunk.home" })
	public void _bucketInColdDirectoryWithNoCopyReceipt_archivesBucket(
			final String shuttlConfDir, final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				TUtilsMBean.runWithRegisteredMBeans(new File(shuttlConfDir),
						new Runnable() {

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
		return TUtilsBucket.createBucketInDirectory(getColdDirectory());
	}

	private File getColdDirectory() {
		return new File(SplunkIndexedLayerFactory.getLoggedInSplunkService()
				.getIndexes().get(TUtilsEndToEnd.REAL_SPLUNK_INDEX)
				.getColdPathExpanded());
	}

	private void executeWarmToColdRetryScript(String splunkHome) {
		HashMap<String, String> env = new HashMap<String, String>();
		env.put("SPLUNK_HOME", splunkHome);
		int exit = ShellExecutor.getInstance().executeCommand(env,
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
		} catch (IOException e) {
			TUtilsTestNG.failForException(null, e);
		}
	}
}
