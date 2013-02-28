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
package com.splunk.shuttl.archiver.distributed;

import static org.testng.Assert.*;

import java.net.URI;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.splunk.shuttl.ShuttlConstants;
import com.splunk.shuttl.archiver.http.JsonRestEndpointCaller;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.util.EndpointUtils;
import com.splunk.shuttl.server.mbeans.util.JsonObjectNames;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "cluster-test" })
public class DistributedListBucketsTest {

	@Parameters(value = { "cluster.slave1.host", "cluster.slave1.shuttl.port",
			"cluster.slave2.host", "cluster.slave2.shuttl.port",
			"cluster.master.host", "cluster.master.shuttl.port",
			"cluster.slave2.splunk.home" })
	public void _searchHeadAndSearchPeers_archivingAtPeersCanBeListedFromSearchHead(
			String peer1Host, String peer1ShuttlPort, String peer2Host,
			String peer2ShuttlPort, String searchHeadHost,
			String searchHeadShuttlPort, String peer2SplunkHome) throws JSONException {
		int shuttlPort = Integer.parseInt(searchHeadShuttlPort);

		LocalBucket b1 = TUtilsBucket.createBucket();
		LocalBucket b2 = TUtilsBucket.createBucketWithIndex(b1.getIndex());
		try {
			DistributedCommons.archiveBucketAtSearchPeer(b1, peer1Host,
					Integer.parseInt(peer1ShuttlPort));
			DistributedCommons.archiveBucketAtSearchPeer(b2, peer2Host,
					Integer.parseInt(peer2ShuttlPort));

			assertBucketsCanBeListedAtMaster(searchHeadHost, shuttlPort, b1, b2);
			assertTotalBucketSizeIsTheSumOfBothBuckets(searchHeadHost, shuttlPort,
					b1, b2);
		} finally {
			DistributedCommons.cleanHadoopFileSystem(peer2SplunkHome);
		}
	}

	private void assertBucketsCanBeListedAtMaster(String searchHeadHost,
			int searchHeadShuttlPort, Bucket... buckets) {
		JSONObject json = jsonFromListBucketEndpoint(searchHeadHost,
				searchHeadShuttlPort, buckets[0].getIndex());
		for (Bucket b : buckets)
			assertTrue(json.toString().contains(b.getName()),
					"JSON" + json.toString() + " did not contain: " + b.getName());
	}

	private JSONObject jsonFromListBucketEndpoint(String searchHeadHost,
			int searchHeadShuttlPort, String index) {
		JsonRestEndpointCaller endpointCaller = new JsonRestEndpointCaller(
				new DefaultHttpClient());
		HttpGet listBucketsRequest = new HttpGet(getListBucketRequestUri(
				searchHeadHost, searchHeadShuttlPort, index));
		return endpointCaller.getJson(listBucketsRequest);
	}

	private URI getListBucketRequestUri(String searchHeadHost,
			int searchHeadShuttlPort, String index) {
		URI listBucketsEndpoint = EndpointUtils.getShuttlEndpointUri(
				searchHeadHost, searchHeadShuttlPort,
				ShuttlConstants.ENDPOINT_LIST_BUCKETS);
		return URI.create(listBucketsEndpoint + "?"
				+ EndpointUtils.createHttpGetParams("index", index));
	}

	private void assertTotalBucketSizeIsTheSumOfBothBuckets(
			String searchHeadHost, int shuttlPort, Bucket... buckets)
			throws JSONException {
		long bucketSize = 0;
		for (Bucket b : buckets)
			bucketSize += b.getSize();

		JSONObject json = jsonFromListBucketEndpoint(searchHeadHost, shuttlPort,
				buckets[0].getIndex());
		assertEquals(bucketSize,
				json.getLong(JsonObjectNames.BUCKET_COLLECTION_SIZE));
	}
}
