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

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Service;
import com.splunk.shuttl.ShuttlConstants;
import com.splunk.shuttl.archiver.archive.BucketDeleter;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.util.EndpointUtils;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEndToEnd;

@Test(groups = { "cluster-test" })
public class DistributedFlushTest {

	private String index = TUtilsEndToEnd.REAL_SPLUNK_INDEX;
	private String splunkUser;
	private String splunkPass;

	@Parameters(value = { "cluster.slave1.host", "cluster.slave1.port",
			"cluster.slave2.host", "cluster.slave2.port", "cluster.master.host",
			"cluster.master.shuttl.port", "splunk.username", "splunk.password" })
	public void _searchHeadAndPeers_canFlushAllPeersFromHead(String peer1Host,
			String peer1Port, String peer2Host, String peer2Port, String headHost,
			String headShuttlPort, String splunkUser, String splunkPass)
			throws IOException {
		this.splunkUser = splunkUser;
		this.splunkPass = splunkPass;

		LocalBucket peer1Bucket = putBucketInPeerThawDirectory(peer1Host, peer1Port);
		LocalBucket peer2Bucket = putBucketInPeerThawDirectory(peer2Host, peer2Port);
		try {
			HttpResponse response = callFlushOnSearchHead(headHost, headShuttlPort);
			assertBucketsExistInFlushResponse(response, peer1Bucket, peer2Bucket);
		} catch (Exception t) {
			deleteBuckets(peer1Bucket, peer2Bucket);
		}
	}

	private LocalBucket putBucketInPeerThawDirectory(String host, String port) {
		File thawDirectory = getThawDirectory(host, port);
		return TUtilsBucket.createBucketInDirectoryWithIndex(thawDirectory, index);
	}

	private File getThawDirectory(String host, String port) {
		Service service = TUtilsEndToEnd.getLoggedInService(host, port, splunkUser,
				splunkPass);
		return new File(service.getIndexes().get(index).getThawedPathExpanded());
	}

	private HttpResponse callFlushOnSearchHead(String headHost, String headPort)
			throws IOException {
		URI flushUri = EndpointUtils.getShuttlEndpointUri(headHost,
				Integer.parseInt(headPort), ShuttlConstants.ENDPOINT_BUCKET_FLUSH);
		HttpPost httpPost = EndpointUtils.createHttpPost(flushUri, "index", index);
		HttpResponse response = new DefaultHttpClient().execute(httpPost);
		assertEquals(200, response.getStatusLine().getStatusCode());
		return response;
	}

	private void assertBucketsExistInFlushResponse(HttpResponse response,
			LocalBucket... buckets) throws IllegalStateException, IOException {
		try {
			String content = IOUtils.toString(response.getEntity().getContent());
			for (LocalBucket b : buckets)
				assertTrue(content.contains(b.getName()));
		} finally {
			EntityUtils.consume(response.getEntity());
		}
	}

	private void deleteBuckets(LocalBucket peer1Bucket, LocalBucket peer2Bucket) {
		BucketDeleter deleter = BucketDeleter.create();
		deleter.deleteBucket(peer1Bucket);
		deleter.deleteBucket(peer2Bucket);
	}

}
