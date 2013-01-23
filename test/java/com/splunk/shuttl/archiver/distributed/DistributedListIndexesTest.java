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

@Test(groups = { "cluster-test" })
public class DistributedListIndexesTest {

	@Parameters(value = { "cluster.slave1.host", "cluster.slave1.shuttl.port",
			"cluster.slave2.host", "cluster.slave2.shuttl.port",
			"cluster.master.host", "cluster.master.shuttl.port",
			"cluster.slave2.splunk.home" })
	public void _archiveTwoBucketsWithDifferentIndexes_listBothIndexesAtSearchHead(
			String peer1Host, String peer1ShuttlPort, String peer2Host,
			String peer2ShuttlPort, String searchHeadHost,
			String searchHeadShuttlPort, String splunkHome) throws JSONException {
		LocalBucket b1 = TUtilsBucket.createBucket();
		LocalBucket b2 = TUtilsBucket.createBucketWithIndex(b1.getIndex() + "x");

		try {
			DistributedCommons.archiveBucketAtSearchPeer(b1, peer1Host,
					Integer.parseInt(peer1ShuttlPort));
			DistributedCommons.archiveBucketAtSearchPeer(b2, peer2Host,
					Integer.parseInt(peer2ShuttlPort));

			JSONObject listedIndexes = listIndexesAtSearchHead(searchHeadHost,
					searchHeadShuttlPort);
			assertBothIndexesWereListed(listedIndexes, b1, b2);
		} finally {
			DistributedCommons.cleanHadoopFileSystem(splunkHome);
		}
	}

	private JSONObject listIndexesAtSearchHead(String searchHeadHost,
			String searchHeadShuttlPort) {
		URI endpointUri = EndpointUtils.getShuttlEndpointUri(searchHeadHost,
				Integer.parseInt(searchHeadShuttlPort),
				ShuttlConstants.ENDPOINT_LIST_INDEXES);
		return JsonRestEndpointCaller.create().getJson(new HttpGet(endpointUri));
	}

	private void assertBothIndexesWereListed(JSONObject json,
			LocalBucket... buckets) throws JSONException {
		for (LocalBucket b : buckets)
			assertTrue(json.get(JsonObjectNames.INDEX_COLLECTION).toString()
					.contains(b.getIndex()));
	}

}
