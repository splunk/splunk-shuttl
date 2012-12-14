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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Service;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.endtoend.util.CopyByCallingCopyScript;
import com.splunk.shuttl.archiver.endtoend.util.CopyByCallingRest;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.filesystem.hadoop.HadoopArchiveFileSystem;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.thaw.SplunkIndexedLayerFactory;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "end-to-end" })
public class CopyWithoutDeletionEndToEndTest {

	public static interface CopiesBucket {
		void copyBucket(LocalBucket bucket);
	}

	@Parameters(value = { "shuttl.host", "shuttl.port", "shuttl.conf.dir",
			"splunk.home" })
	public void _callingCopyRestEndpointWithBucket_copiesTheBucketToStorageWithoutDeletingOriginal(
			final String shuttlHost, final String shuttlPort,
			final String shuttlConfDir, final String splunkHome) {
		CopyByCallingRest copyByCallingRest = new CopyByCallingRest(shuttlHost,
				shuttlPort);
		runTestWithSplunkHomeSet(shuttlConfDir, splunkHome, copyByCallingRest);
	}

	@Parameters(value = { "shuttl.conf.dir", "splunk.home" })
	public void _callingCopyScriptWithBucket_copiesTheBucketToStorageWithoutDeletingOriginal(
			String shuttlConfDir, String splunkHome) {
		CopiesBucket copyWithCopyScript = new CopyByCallingCopyScript(splunkHome);
		runTestWithSplunkHomeSet(shuttlConfDir, splunkHome, copyWithCopyScript);
	}

	private void runTestWithSplunkHomeSet(final String shuttlConfDir,
			final String splunkHome, final CopiesBucket copiesBucket) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				doRunTestWithSplunkHomeSet(shuttlConfDir, copiesBucket);
			}
		});
	}

	private void doRunTestWithSplunkHomeSet(String shuttlConfDir,
			final CopiesBucket bucketCopier) {
		TUtilsMBean.runWithRegisteredMBeans(new File(shuttlConfDir),
				new Runnable() {

					@Override
					public void run() {
						Service service = SplunkIndexedLayerFactory.getLoggedInSplunkService();
						String index = "shuttl";
						assertTrue(service.getIndexes().containsKey(index));
						LocalBucket bucket = TUtilsBucket.createBucketInDirectoryWithIndex(
								createDirectory(), index);
						assertTrue(service.getIndexes().containsKey(bucket.getIndex()));

						bucketCopier.copyBucket(bucket);
						assertTrue(bucket.getDirectory().exists());

						assertBucketWasCopiedToArchiveFileSystem_withTeardown(bucket);
					}
				});
	}

	private void assertBucketWasCopiedToArchiveFileSystem_withTeardown(
			LocalBucket bucket) {
		ArchiveConfiguration config = ArchiveConfiguration
				.createConfigurationFromMBean();
		HadoopArchiveFileSystem fileSystem = (HadoopArchiveFileSystem) ArchiveFileSystemFactory
				.getWithConfiguration(config);
		PathResolver pathResolver = new PathResolver(config);

		String bucketArchivePath = pathResolver.resolveArchivePath(bucket);
		try {
			assertTrue(fileSystem.exists(bucketArchivePath),
					"BucketArchivePath did not exist: " + bucketArchivePath);
		} catch (IOException e) {
			TUtilsTestNG.failForException(
					"Checking for existing bucket path throwed.", e);
		} finally {
			fileSystem.deletePath(bucketArchivePath);
		}
	}
}
