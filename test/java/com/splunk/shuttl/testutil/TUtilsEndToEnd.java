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
package com.splunk.shuttl.testutil;

import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;

import com.splunk.Service;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.filesystem.hadoop.HadoopArchiveFileSystem;
import com.splunk.shuttl.server.mbeans.util.EndpointUtils;

public class TUtilsEndToEnd {

	public static final String REAL_SPLUNK_INDEX = "shuttl";

	public static void callSlaveArchiveBucketEndpoint(String index,
			String bucketPath, String host, int shuttlPort) {
		HttpPost httpPost = EndpointUtils.createArchiveBucketPostRequest(host,
				shuttlPort, bucketPath, index);
		DefaultHttpClient httpClient = new DefaultHttpClient();
		HttpResponse response = executeHttp(httpPost, httpClient);

		assertEquals(204, response.getStatusLine().getStatusCode());
	}

	private static HttpResponse executeHttp(HttpPost httpPost,
			DefaultHttpClient httpClient) {
		try {
			return httpClient.execute(httpPost);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Given shuttl conf dir that's configured to archive with hadoop -> cleans
	 * the hadoop archive file system.
	 */
	public static void cleanHadoopFileSystem(final File shuttlConfDir,
			final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				cleanHadoopFileSystemWithSplunkHomeSet(shuttlConfDir);
			}
		});
	}

	private static void cleanHadoopFileSystemWithSplunkHomeSet(File shuttlConfDir) {
		TUtilsMBean.runWithRegisteredMBeans(shuttlConfDir, new Runnable() {

			@Override
			public void run() {
				HadoopArchiveFileSystem fs = (HadoopArchiveFileSystem) ArchiveFileSystemFactory
						.getConfiguredArchiveFileSystem();
				PathResolver pathResolver = new PathResolver(ArchiveConfiguration
						.createConfigurationFromMBean());
				fs.deletePath(pathResolver.getServerNamesHome());
			}
		});
	}

	public static File getShuttlConfDirFromService(Service splunkService) {
		String splunkHome = splunkService.getSettings().getSplunkHome();
		return getShuttlConfDirFromSplunkHome(splunkHome);
	}

	public static File getShuttlConfDirFromSplunkHome(String splunkHome) {
		return new File(splunkHome + "/etc/apps/shuttl/conf");
	}

	public static Service getLoggedInService(String host, String port,
			String splunkUser, String splunkPass) {
		return getLoggedInService(host, Integer.parseInt(port), splunkUser,
				splunkPass);
	}

	public static Service getLoggedInService(String host, int port,
			String splunkUser, String splunkPass) {
		Service masterService = new Service(host, port);
		masterService.login(splunkUser, splunkPass);
		return masterService;
	}
}
