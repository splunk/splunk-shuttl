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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.IOException;
import java.net.URI;

import org.apache.http.client.methods.HttpPost;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.splunk.shuttl.ShuttlConstants;
import com.splunk.shuttl.archiver.http.JsonRestEndpointCaller;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.util.EndpointUtils;
import com.splunk.shuttl.server.mbeans.util.JsonObjectNames;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEndToEnd;

@Test(groups = { "cluster-test" })
public class DistributedThawTest {

	@Parameters(value = { "cluster.slave1.host", "cluster.slave1.shuttl.port",
			"cluster.slave2.host", "cluster.slave2.shuttl.port",
			"cluster.master.host", "cluster.master.shuttl.port",
			"cluster.slave2.splunk.home" })
	public void _searcHeadAndPeers_thawBucketsByCallingSearchHead(
			String peer1Host, String peer1ShuttlPort, String peer2Host,
			String peer2ShuttlPort, String searchHeadHost,
			String searchHeadShuttlPort, String splunkHome) throws IOException,
			JSONException {

		String index = TUtilsEndToEnd.REAL_SPLUNK_INDEX;
		LocalBucket b1 = TUtilsBucket.createBucketInDirectoryWithIndex(
				createDirectory(), index);
		LocalBucket b2 = TUtilsBucket.createBucketInDirectoryWithIndex(
				createDirectory(), index);

		try {
			DistributedCommons.archiveBucketAtSearchPeer(b1, peer1Host,
					Integer.parseInt(peer1ShuttlPort));
			DistributedCommons.archiveBucketAtSearchPeer(b2, peer2Host,
					Integer.parseInt(peer2ShuttlPort));

			JSONObject json = thawBuckets(searchHeadHost, searchHeadShuttlPort,
					b1.getIndex());
			assertBucketsWereThawed(json, b1, b2);
		} finally {
			DistributedCommons.cleanHadoopFileSystem(splunkHome);
		}
	}

	private JSONObject thawBuckets(String host, String shuttlPort, String index)
			throws IOException {
		URI thawEndpoint = EndpointUtils.getShuttlEndpointUri(host,
				Integer.parseInt(shuttlPort), ShuttlConstants.ENDPOINT_BUCKET_THAW);
		HttpPost httpPost = EndpointUtils.createHttpPost(thawEndpoint, "index",
				index);
		return JsonRestEndpointCaller.create().getJson(httpPost);
	}

	private void assertBucketsWereThawed(JSONObject json, LocalBucket... buckets)
			throws JSONException {
		for (LocalBucket b : buckets)
			assertTrue(json.get(JsonObjectNames.BUCKET_COLLECTION).toString()
					.contains(b.getName()), "JSON" + json.toString()
					+ " did not contain: " + b.getName());
	}
}
