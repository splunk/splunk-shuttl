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
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Service;
import com.splunk.shuttl.ShuttlConstants;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.testutil.TUtilsHttp;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "cluster-test" })
public class ClusterReplicatedBucketArchivingTest {

	private String index;

	@Parameters(value = { "cluster.slave1.host", "cluster.slave1.port",
			"cluster.slave2.host", "cluster.slave2.port",
			"cluster.slave2.shuttl.port", "splunk.username", "splunk.password",
			"splunk.home" })
	public void test(String slave1Host, String slave1Port, String slave2Host,
			String slave2Port, String slave2ShuttlPort, String splunkUser,
			String splunkPass, final String splunkHome) {
		index = "shuttl";
		Service slave1 = getLoggedInService(slave1Host, slave1Port, splunkUser,
				splunkPass);
		Service slave2 = getLoggedInService(slave2Host, slave2Port, splunkUser,
				splunkPass);

		assertTrue(slave2.getIndexes().containsKey(index));
		String coldPathExpanded = slave2.getIndexes().get(index)
				.getColdPathExpanded();

		String slave1Guid = slave1.getInfo().getGuid();

		final LocalBucket rb = TUtilsBucket.createReplicatedBucket(index, new File(
				coldPathExpanded), slave1Guid);

		try {
			callSlave2ArchiveBucketEndpoint(index, rb.getDirectory()
					.getAbsolutePath(), slave2Host, slave2ShuttlPort);
		} catch (IOException e1) {
			TUtilsTestNG.failForException(
					"failed when calling archive bucket endpoint", e1);
		}

		String slave1SplunkHome = slave1.getSettings().getSplunkHome();
		final File slave1ShuttlConfDir = new File(slave1SplunkHome
				+ "/etc/apps/shuttl/conf");
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				TUtilsMBean.runWithRegisteredMBeans(slave1ShuttlConfDir,
						new AssertBucketWasArchived(rb));
			}
		});
	}

	private static class AssertBucketWasArchived implements Runnable {
		private LocalBucket replicatedBucket;

		public AssertBucketWasArchived(LocalBucket rb) {
			this.replicatedBucket = rb;
		}

		@Override
		public void run() {
			ArchiveConfiguration config = ArchiveConfiguration
					.createConfigurationFromMBean();
			PathResolver pathResolver = new PathResolver(config);
			String archivePath = pathResolver.resolveArchivePath(replicatedBucket);
			ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
					.getWithConfiguration(config);
			try {
				assertTrue(archiveFileSystem.exists(archivePath));
			} catch (IOException e) {
				TUtilsTestNG.failForException("Path did not exist: " + archivePath, e);
			}
		}
	}

	private Service getLoggedInService(String slave2Host, String slave2Port,
			String splunkUser, String splunkPass) {
		Service slave2 = new Service(slave2Host, Integer.parseInt(slave2Port));
		slave2.login(splunkUser, splunkPass);
		return slave2;
	}

	private void callSlave2ArchiveBucketEndpoint(String index, String bucketPath,
			String slave2Host, String slave2ShuttlPort)
			throws ClientProtocolException, IOException {
		HttpPost httpPost = new HttpPost("http://" + slave2Host + ":"
				+ slave2ShuttlPort + "/" + ShuttlConstants.ENDPOINT_CONTEXT
				+ ShuttlConstants.ENDPOINT_ARCHIVER
				+ ShuttlConstants.ENDPOINT_BUCKET_ARCHIVER);
		List<BasicNameValuePair> postParams = asList(new BasicNameValuePair(
				"index", index), new BasicNameValuePair("path", bucketPath));
		TUtilsHttp.setParamsToPostRequest(httpPost, postParams);
		DefaultHttpClient httpClient = new DefaultHttpClient();
		HttpResponse response = httpClient.execute(httpPost);

		assertEquals(204, response.getStatusLine().getStatusCode());
	}
}
