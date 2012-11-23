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
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.testutil.TUtilsHttp;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "end-to-end" })
public class CopyWithoutDeletionEndToEndTest {

	@Parameters(value = { "shuttl.host", "shuttl.port", "shuttl.conf.dir",
			"splunk.home" })
	public void _callingCopyRestEndpointWithBucket_copiesTheBucketToStorageWithoutDeletingOriginal(
			final String shuttlHost, final String shuttlPort,
			final String shuttlConfDir, final String splunkHome)
			throws ClientProtocolException, IOException {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				try {
					runTestWithSplunkHomeSet(shuttlHost, shuttlPort, shuttlConfDir);
				} catch (IOException e) {
					TUtilsTestNG.failForException("Test got exception.", e);
				}
			}
		});
	}

	private void runTestWithSplunkHomeSet(String shuttlHost, String shuttlPort,
			String shuttlConfDir) throws IOException, ClientProtocolException {
		final LocalBucket bucket = TUtilsBucket.createBucket();

		HttpPost postRequest = createPostRequest(shuttlHost, shuttlPort, bucket);
		HttpResponse httpResponse = new DefaultHttpClient().execute(postRequest);

		int statusCode = httpResponse.getStatusLine().getStatusCode();
		assertTrue(300 > statusCode,
				"Http endpoint status code not less than 300. Was: " + statusCode);
		assertTrue(bucket.getDirectory().exists());

		TUtilsMBean.runWithRegisteredMBeans(new File(shuttlConfDir),
				new Runnable() {

					@Override
					public void run() {
						ArchiveConfiguration config = ArchiveConfiguration
								.createConfigurationFromMBean();
						ArchiveFileSystem fileSystem = ArchiveFileSystemFactory
								.getWithConfiguration(config);
						String bucketArchivePath = new PathResolver(config)
								.resolveArchivePath(bucket);
						try {
							assertTrue(fileSystem.exists(bucketArchivePath));
						} catch (IOException e) {
							TUtilsTestNG.failForException(
									"Checking for existing bucket path throwed.", e);
						}
					}
				});
	}

	private HttpPost createPostRequest(String shuttlHost, String shuttlPort,
			LocalBucket bucket) {
		URI copyBucketEndpoint = URI.create("http://" + shuttlHost + ":"
				+ shuttlPort + "/" + ENDPOINT_CONTEXT + ENDPOINT_ARCHIVER
				+ ENDPOINT_BUCKET_COPY);
		HttpPost postRequest = TUtilsHttp.createHttpPost(copyBucketEndpoint,
				"path", bucket.getDirectory().getAbsolutePath(), "index",
				bucket.getIndex());
		return postRequest;
	}
}
