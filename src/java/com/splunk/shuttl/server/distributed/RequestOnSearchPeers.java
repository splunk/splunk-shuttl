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
package com.splunk.shuttl.server.distributed;

import static com.splunk.shuttl.archiver.LogFormatter.*;
import static java.util.Arrays.*;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.amazonaws.util.json.JSONObject;
import com.splunk.DistributedPeer;
import com.splunk.EntityCollection;
import com.splunk.Service;
import com.splunk.shuttl.archiver.http.JsonRestEndpointCaller;
import com.splunk.shuttl.archiver.thaw.SplunkConfiguration;
import com.splunk.shuttl.archiver.thaw.SplunkIndexedLayerFactory;

/**
 * Makes requests on the distributed peers connected to the Shuttl's Splunk.
 */
public class RequestOnSearchPeers {

	private static final Logger logger = Logger
			.getLogger(RequestOnSearchPeers.class);

	private final Service splunkService;
	private final RequestOnSearchPeer requestOnSearchPeer;

	private final Queue<RuntimeException> exceptions;

	public RequestOnSearchPeers(Service splunkService,
			RequestOnSearchPeer requestOnSearchPeer) {
		this.splunkService = splunkService;
		this.requestOnSearchPeer = requestOnSearchPeer;
		this.exceptions = new ConcurrentLinkedQueue<RuntimeException>();
	}

	public List<RuntimeException> getExceptions() {
		return queueToList(exceptions, new RuntimeException[0]);
	}

	private <T> List<T> queueToList(Queue<T> jsons, T[] t) {
		return asList(jsons.toArray(t));
	}

	/**
	 * @return JSONObjects as response from each distributed peer.
	 */
	public List<JSONObject> execute() {
		return requestOnSearchPeersInParallel();
	}

	private List<JSONObject> requestOnSearchPeersInParallel() {
		final Queue<JSONObject> jsons = new ConcurrentLinkedQueue<JSONObject>();
		exceptions.clear();

		EntityCollection<DistributedPeer> distributedPeers = splunkService
				.getDistributedPeers();
		if (distributedPeers != null) {
			ExecutorService executorService = Executors.newCachedThreadPool();
			executeRequestsInParallel(jsons, distributedPeers, executorService);
			joinRequests(executorService);
		}
		return queueToList(jsons, new JSONObject[0]);
	}

	private void executeRequestsInParallel(final Queue<JSONObject> jsons,
			EntityCollection<DistributedPeer> distributedPeers,
			ExecutorService executorService) {
		for (final DistributedPeer dp : distributedPeers.values())
			executorService.submit(new Runnable() {
				@Override
				public void run() {
					executeRequestOnPeer(dp, jsons);
				}
			}, null);
	}

	private void executeRequestOnPeer(DistributedPeer dp, Queue<JSONObject> jsons) {
		try {
			JSONObject json = requestOnSearchPeer.executeRequest(dp);
			if (!Thread.currentThread().isInterrupted())
				jsons.offer(json);
		} catch (RuntimeException e) {
			logger.warn(warn("Executed request on distributed peer", e,
					"will add to exceptions, which can be "
							+ "retrieved with getExceptions()"));
			exceptions.offer(e);
		}
	}

	private void joinRequests(ExecutorService executorService) {
		try {
			executorService.shutdown();
			executorService.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.warn(warn("waited for executor to finish", e, "will do nothing"));
		} finally {
			executorService.shutdownNow();
		}
	}

	public static RequestOnSearchPeers createPost(String endpoint, String index,
			String from, String to) {
		return create(new PostRequestProvider(endpoint, index, from, to));
	}

	public static RequestOnSearchPeers createGet(String endpoint, String index,
			String from, String to) {
		return create(new GetRequestProvider(endpoint, index, from, to));
	}

	private static RequestOnSearchPeers create(
			ShuttlEndpointRequestProvider requestProvider) {
		Service splunkService = SplunkIndexedLayerFactory
				.getLoggedInSplunkService();
		return new RequestOnSearchPeers(splunkService, new RequestOnSearchPeer(
				requestProvider, JsonRestEndpointCaller.create(),
				SplunkConfiguration.create()));
	}
}
