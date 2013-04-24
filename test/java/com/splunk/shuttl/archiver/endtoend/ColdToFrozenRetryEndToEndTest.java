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

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.recovery.IndexPreservingBucketMover;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "end-to-end" })
public class ColdToFrozenRetryEndToEndTest {

	private final Runnable runTestWithEnvironmentAndConf = new Runnable() {

		@Override
		public void run() {
			try {
				coldToFrozenRetryScript_bucketInSafeDirectory_whenExecutedBucketGetsArchived();
			} catch (Exception e) {
				TUtilsTestNG.failForException(null, e);
			}
		}
	};

	@Parameters(value = { "shuttl.conf.dir", "splunk.home" })
	public void _(final String shuttlConfDir, final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", new File(
						splunkHome).getAbsolutePath());
				TUtilsMBean.runWithRegisteredMBeans(new File(shuttlConfDir),
						runTestWithEnvironmentAndConf);
			}
		});
	}

	private void coldToFrozenRetryScript_bucketInSafeDirectory_whenExecutedBucketGetsArchived()
			throws IOException {
		LocalBucket bucket = putBucketInSafeDirectory();

		File coldToFrozenRetryScript = getScript();

		executeCommand(coldToFrozenRetryScript);

		asserBucketWasArchived(bucket);
	}

	private LocalBucket putBucketInSafeDirectory() {
		File safeDir = LocalFileSystemPaths.create().getSafeDirectory();
		IndexPreservingBucketMover bucketMover = IndexPreservingBucketMover
				.create(safeDir);
		LocalBucket bucket = TUtilsBucket.createBucket();
		bucketMover.moveBucket(bucket);
		return bucket;
	}

	private File getScript() {
		String coldToFrozenRetryScriptName = "coldToFrozenRetry.sh";
		File coldToFrozenRetryScript = new File(System.getenv("SPLUNK_HOME"),
				"/etc/apps/shuttl/bin/" + coldToFrozenRetryScriptName);
		assertTrue(coldToFrozenRetryScript.canExecute());
		return coldToFrozenRetryScript;
	}

	private void executeCommand(File coldToFrozenRetryScript) {
		ShellExecutor executor = ShellExecutor.getInstance();
		HashMap<String, String> env = new HashMap<String, String>();
		env.put("SPLUNK_HOME", System.getenv("SPLUNK_HOME"));
		int exit = executor.executeCommand(env,
				asList(coldToFrozenRetryScript.getAbsolutePath()));
		assertEquals(exit, 0);
	}

	private void asserBucketWasArchived(LocalBucket bucket) throws IOException {
		ArchiveConfiguration config = ArchiveConfiguration
				.createConfigurationFromMBean();
		String archivePath = new PathResolver(config).resolveArchivePath(bucket);
		assertTrue(ArchiveFileSystemFactory.getWithConfiguration(config).exists(
				archivePath));
	}
}
