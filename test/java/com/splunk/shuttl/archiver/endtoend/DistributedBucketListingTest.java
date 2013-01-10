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

import static org.testng.Assert.*;

import java.io.File;
import java.net.URI;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.amazonaws.util.json.JSONObject;
import com.splunk.shuttl.ShuttlConstants;
import com.splunk.shuttl.archiver.http.JsonRestEndpointCaller;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.util.EndpointUtils;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEndToEnd;

@Test(groups = { "cluster-test" })
public class DistributedBucketListingTest {

	@Parameters(value = { "cluster.slave1.host", "cluster.slave1.shuttl.port",
			"cluster.slave2.host", "cluster.slave2.shuttl.port",
			"cluster.master.host", "cluster.master.shuttl.port",
			"cluster.slave2.splunk.home" })
	public void _searchHeadAndSearchPeers_archivingAtPeersCanBeListedFromSearchHead(
			String peer1Host, String peer1ShuttlPort, String peer2Host,
			String peer2ShuttlPort, String searchHeadHost,
			String searchHeadShuttlPort, String peer2SplunkHome) {

		try {
			Bucket peer1Bucket = archiveBucketAtSearchPeer(peer1Host,
					Integer.parseInt(peer1ShuttlPort));
			Bucket peer2Bucket = archiveBucketAtSearchPeer(peer2Host,
					Integer.parseInt(peer2ShuttlPort));

			assertBucketCanBeListedAtMaster(searchHeadHost,
					Integer.parseInt(searchHeadShuttlPort), peer1Bucket);
			assertBucketCanBeListedAtMaster(searchHeadHost,
					Integer.parseInt(searchHeadShuttlPort), peer2Bucket);
		} finally {
			File shuttlConfDir = TUtilsEndToEnd
					.getShuttlConfDirFromSplunkHome(peer2SplunkHome);
			TUtilsEndToEnd.cleanHadoopFileSystem(shuttlConfDir, peer2SplunkHome);
		}
	}

	private Bucket archiveBucketAtSearchPeer(String slave1Host,
			int slave1ShuttlPort) {
		LocalBucket b = TUtilsBucket.createBucket();
		TUtilsEndToEnd.callSlaveArchiveBucketEndpoint(b.getIndex(), b
				.getDirectory().getAbsolutePath(), slave1Host, slave1ShuttlPort);
		return b;
	}

	private void assertBucketCanBeListedAtMaster(String searchHeadHost,
			int searchHeadShuttlPort, Bucket bucket) {
		JsonRestEndpointCaller endpointCaller = new JsonRestEndpointCaller(
				new DefaultHttpClient());
		HttpGet listBucketsRequest = new HttpGet(getListBucketRequestUri(
				searchHeadHost, searchHeadShuttlPort, bucket.getIndex()));
		JSONObject json = endpointCaller.getJson(listBucketsRequest);
		assertTrue(json.toString().contains(bucket.getName()));
	}

	private URI getListBucketRequestUri(String searchHeadHost,
			int searchHeadShuttlPort, String index) {
		URI listBucketsEndpoint = EndpointUtils.getShuttlEndpointUri(
				searchHeadHost, searchHeadShuttlPort,
				ShuttlConstants.ENDPOINT_LIST_BUCKETS);
		return URI.create(listBucketsEndpoint + "?"
				+ EndpointUtils.createHttpGetParams("index", index));
	}
}
