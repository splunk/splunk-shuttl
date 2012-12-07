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
package com.splunk.shuttl.archiver.endtoend.util;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static java.util.Arrays.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.splunk.Index;
import com.splunk.Service;
import com.splunk.shuttl.archiver.endtoend.CopyWithoutDeletionEndToEndTest;
import com.splunk.shuttl.archiver.endtoend.CopyWithoutDeletionEndToEndTest.CopiesBucket;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.thaw.SplunkSettingsFactory;
import com.splunk.shuttl.testutil.TUtilsTestNG;

/**
 * Used for testing {@link CopyWithoutDeletionEndToEndTest}
 */
public class CopyByCallingCopyScript implements CopiesBucket {

	private String splunkHome;

	public CopyByCallingCopyScript(String splunkHome) {
		this.splunkHome = splunkHome;
	}

	@Override
	public void copyBucket(LocalBucket bucket) {
		Service splunkService = SplunkSettingsFactory.getLoggedInSplunkService();
		Index index = splunkService.getIndexes().get(bucket.getIndex());
		File indexColdDir = new File(index.getColdPathExpanded());
		assertTrue(indexColdDir.exists());

		File copyScript = getCopyScript();
		File movedBucketDirectory = createDirectoryInParent(indexColdDir,
				bucket.getName());
		movedBucketDirectory.delete();

		executeCopyScript(bucket, copyScript, movedBucketDirectory);
		assertThatTheOriginalBucketWasMovedByTheScript(bucket, movedBucketDirectory);

		moveOriginalBucketBackToItsFirstLocation(movedBucketDirectory, bucket);
	}

	private void executeCopyScript(LocalBucket bucket, File copyScript,
			File movedBucketDirectory) {
		ShellExecutor shellExecutor = ShellExecutor.getInstance();
		Map<String, String> env = new HashMap<String, String>();
		env.put("SPLUNK_HOME", new File(splunkHome).getAbsolutePath());
		List<String> command = createCommand(bucket, copyScript,
				movedBucketDirectory);
		int exit = shellExecutor.executeCommand(env, command);
		assertEquals(exit, 0);
	}

	private List<String> createCommand(LocalBucket bucket, File copyScript,
			File movedBucketDirectory) {
		String scriptPath = copyScript.getAbsolutePath();
		String bucketPath = bucket.getDirectory().getAbsolutePath();
		String dirPath = movedBucketDirectory.getAbsolutePath();
		return asList(scriptPath, bucketPath, dirPath);
	}

	private File getCopyScript() {
		String copyScriptPath = splunkHome + "/etc/apps/shuttl/bin/copyBucket.sh";
		File copyScript = new File(copyScriptPath);
		assertTrue(copyScript.exists());
		assertTrue(copyScript.canExecute());
		return copyScript;
	}

	private void assertThatTheOriginalBucketWasMovedByTheScript(
			LocalBucket bucket, File movedBucketDirectory) {
		assertFalse(bucket.getDirectory().exists());
		LocalBucket movedBucket = createBucketFromDirectory(bucket,
				movedBucketDirectory);
		TUtilsTestNG.isBucketEqualOnIndexFormatAndName(bucket, movedBucket);
	}

	private LocalBucket createBucketFromDirectory(LocalBucket bucket,
			File movedBucketDirectory) {
		try {
			return new LocalBucket(movedBucketDirectory, bucket.getIndex(),
					bucket.getFormat());
		} catch (Exception e) {
			TUtilsTestNG.failForException("Could not create bucket", e);
			return null;
		}
	}

	private void moveOriginalBucketBackToItsFirstLocation(
			File directoryToMoveBucketTo, LocalBucket bucket) {
		directoryToMoveBucketTo.renameTo(bucket.getDirectory());
	}
}
