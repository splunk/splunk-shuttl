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

import java.io.IOException;
import java.net.URI;

import org.apache.http.client.methods.HttpGet;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.splunk.shuttl.ShuttlConstants;
import com.splunk.shuttl.archiver.http.JsonRestEndpointCaller;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.util.EndpointUtils;
import com.splunk.shuttl.server.mbeans.util.JsonObjectNames;
import com.splunk.shuttl.testutil.TUtilsEndToEnd;

@Test(groups = { "cluster-test" })
public class DistributedListThawedTest {

	@Parameters(value = { "cluster.slave1.host", "cluster.slave1.port",
			"cluster.slave2.host", "cluster.slave2.port", "cluster.master.host",
			"cluster.master.shuttl.port", "splunk.username", "splunk.password" })
	public void _bucketsInThawedAtPeers_canListThawedBucketsFromMaster(
			String peer1Host, String peer1Port, String peer2Host, String peer2Port,
			String headHost, String headShuttlPort, String splunkUser,
			String splunkPass) throws IOException, JSONException {
		String index = TUtilsEndToEnd.REAL_SPLUNK_INDEX;

		LocalBucket peer1Bucket = DistributedCommons.putBucketInPeerThawDirectory(
				peer1Host, peer1Port, splunkUser, splunkPass, index);
		LocalBucket peer2Bucket = DistributedCommons.putBucketInPeerThawDirectory(
				peer2Host, peer2Port, splunkUser, splunkPass, index);

		try {
			JSONObject json = listThawedByCallingHead(headHost, headShuttlPort, index);
			assertBucketsWereListed(json, peer1Bucket, peer2Bucket);
		} finally {
			DistributedCommons.deleteBuckets(peer1Bucket, peer2Bucket);
		}
	}

	private JSONObject listThawedByCallingHead(String headHost,
			String headShuttlPort, String index) {
		URI endpointUri = EndpointUtils.getShuttlEndpointUri(headHost,
				Integer.parseInt(headShuttlPort), ShuttlConstants.ENDPOINT_THAW_LIST);
		String getParams = EndpointUtils.createHttpGetParams("index", index);
		return JsonRestEndpointCaller.create().getJson(
				new HttpGet(URI.create(endpointUri + "?" + getParams)));
	}

	private void assertBucketsWereListed(JSONObject json, LocalBucket... buckets)
			throws JSONException {
		for (LocalBucket b : buckets)
			assertTrue(json.get(JsonObjectNames.BUCKET_COLLECTION).toString()
					.contains(b.getName()));
	}
}
