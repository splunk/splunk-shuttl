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

import static com.splunk.shuttl.ShuttlConstants.*;
import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static java.util.Arrays.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.filesystem.hadoop.HadoopArchiveFileSystem;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.testutil.TUtilsHttp;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "end-to-end" })
public class CopyWithoutDeletionEndToEndTest {

	private static interface CopiesBucket {
		void copyBucket(LocalBucket bucket);
	}

	private static class CopyByCallingRest implements CopiesBucket {

		private String shuttlHost;
		private String shuttlPort;

		public CopyByCallingRest(String shuttlHost, String shuttlPort) {
			this.shuttlHost = shuttlHost;
			this.shuttlPort = shuttlPort;
		}

		@Override
		public void copyBucket(LocalBucket bucket) {
			try {
				copyBucketViaRestCall(shuttlHost, shuttlPort, bucket);
			} catch (Exception e) {
				TUtilsTestNG.failForException("Got exception when copying bucket.", e);
			}
		}

		private void copyBucketViaRestCall(String shuttlHost, String shuttlPort,
				final LocalBucket bucket) throws IOException, ClientProtocolException {
			HttpPost copyBucketRequest = createCopyBucketPostRequest(shuttlHost,
					shuttlPort, bucket);
			HttpResponse httpResponse = new DefaultHttpClient()
					.execute(copyBucketRequest);

			int statusCode = httpResponse.getStatusLine().getStatusCode();
			assertTrue(300 > statusCode,
					"Http endpoint status code not less than 300. Was: " + statusCode);
		}

		private HttpPost createCopyBucketPostRequest(String shuttlHost,
				String shuttlPort, LocalBucket bucket) {
			URI copyBucketEndpoint = URI.create("http://" + shuttlHost + ":"
					+ shuttlPort + "/" + ENDPOINT_CONTEXT + ENDPOINT_ARCHIVER
					+ ENDPOINT_BUCKET_COPY);
			HttpPost postRequest = TUtilsHttp.createHttpPost(copyBucketEndpoint,
					"path", bucket.getDirectory().getAbsolutePath(), "index",
					bucket.getIndex());
			return postRequest;
		}
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
	@Test(enabled = false)
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

	private static class CopyByCallingCopyScript implements CopiesBucket {

		private String splunkHome;

		public CopyByCallingCopyScript(String splunkHome) {
			this.splunkHome = splunkHome;
		}

		@Override
		public void copyBucket(LocalBucket bucket) {
			File copyScript = getCopyScript();
			File directoryToMoveBucketTo = createDirectory();

			executeCopyScript(bucket, copyScript, directoryToMoveBucketTo);
			assertThatTheOriginalBucketWasMovedByTheScript(bucket,
					directoryToMoveBucketTo);

			moveOriginalBucketBackToItsFirstLocation(directoryToMoveBucketTo, bucket);
		}

		private void executeCopyScript(LocalBucket bucket, File copyScript,
				File directoryToMoveBucketTo) {
			ShellExecutor shellExecutor = ShellExecutor.getInstance();
			Map<String, String> env = new HashMap<String, String>();
			env.put("SPLUNK_HOME", splunkHome);
			List<String> command = createCommand(bucket, copyScript,
					directoryToMoveBucketTo);
			int exit = shellExecutor.executeCommand(env, command);
			System.out.println(shellExecutor.getStdErr());
			assertEquals(exit, 0);
		}

		private List<String> createCommand(LocalBucket bucket, File copyScript,
				File directoryToMoveBucketTo) {
			String scriptPath = copyScript.getAbsolutePath();
			String bucketPath = bucket.getDirectory().getAbsolutePath();
			String dirPath = directoryToMoveBucketTo.getAbsolutePath();
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
				LocalBucket bucket, File directoryToMoveBucketTo) {
			assertFalse(bucket.getDirectory().exists());
			File[] filesInNewDir = directoryToMoveBucketTo.listFiles();
			assertEquals(filesInNewDir.length, 1);
			File movedBucket = filesInNewDir[0];
			File aRealBucket = TUtilsBucket.createRealBucket().getDirectory();
			TUtilsTestNG.assertDirectoriesAreCopies(movedBucket, aRealBucket);
		}

		private void moveOriginalBucketBackToItsFirstLocation(
				File directoryToMoveBucketTo, LocalBucket bucket) {
			directoryToMoveBucketTo.renameTo(bucket.getDirectory());
		}
	}

	private void doRunTestWithSplunkHomeSet(String shuttlConfDir,
			CopiesBucket bucketCopier) {
		final LocalBucket bucket = TUtilsBucket.createRealBucket();
		bucketCopier.copyBucket(bucket);
		assertTrue(bucket.getDirectory().exists());

		assertBucketWasCopied(shuttlConfDir, bucket);
	}

	private void assertBucketWasCopied(String shuttlConfDir,
			final LocalBucket bucket) {
		TUtilsMBean.runWithRegisteredMBeans(new File(shuttlConfDir),
				new AssertBucketWasCopiedToArchiveFileSystem_withTeardown(bucket));
	}

	private static class AssertBucketWasCopiedToArchiveFileSystem_withTeardown
			implements Runnable {
		private final LocalBucket bucket;

		public AssertBucketWasCopiedToArchiveFileSystem_withTeardown(
				LocalBucket bucket) {
			this.bucket = bucket;
		}

		@Override
		public void run() {
			ArchiveConfiguration config = ArchiveConfiguration
					.createConfigurationFromMBean();
			HadoopArchiveFileSystem fileSystem = (HadoopArchiveFileSystem) ArchiveFileSystemFactory
					.getWithConfiguration(config);
			PathResolver pathResolver = new PathResolver(config);

			String bucketArchivePath = pathResolver.resolveArchivePath(bucket);
			try {
				assertTrue(fileSystem.exists(bucketArchivePath));
			} catch (IOException e) {
				TUtilsTestNG.failForException(
						"Checking for existing bucket path throwed.", e);
			} finally {
				fileSystem.deletePath(bucketArchivePath);
			}
		}
	}
}
