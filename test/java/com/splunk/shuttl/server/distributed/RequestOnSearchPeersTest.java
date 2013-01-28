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

import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazonaws.util.json.JSONObject;
import com.splunk.DistributedPeer;
import com.splunk.Service;

@Test(groups = { "fast-test" })
public class RequestOnSearchPeersTest {

	private RequestOnSearchPeers requestOnSearchPeers;
	private RequestOnSearchPeer requestOnSearchPeer;
	private Service splunkService;

	@BeforeMethod
	public void setUp() {
		splunkService = mock(Service.class, RETURNS_DEEP_STUBS);
		requestOnSearchPeer = mock(RequestOnSearchPeer.class);
		requestOnSearchPeers = new RequestOnSearchPeers(splunkService,
				requestOnSearchPeer);
	}

	public void execute_successfulRequests_returnListOfTheirJson() {
		mockServiceToReturnPeers(mock(DistributedPeer.class),
				mock(DistributedPeer.class));
		when(requestOnSearchPeer.executeRequest(any(DistributedPeer.class)))
				.thenReturn(new JSONObject());

		List<JSONObject> jsons = requestOnSearchPeers.execute();
		assertEquals(jsons.size(), 2);
	}

	private void mockServiceToReturnPeers(DistributedPeer... peers) {
		when(splunkService.getDistributedPeers().values())
				.thenReturn(asList(peers));
	}

	public void execute_noDistributedPeers_emptyList() {
		mockServiceToReturnPeers();
		List<JSONObject> actual = requestOnSearchPeers.execute();
		assertEquals(actual, new ArrayList<DistributedPeer>());
	}

	public void execute_onePeerThatThrows_emptyList() {
		DistributedPeer failingPeer = mock(DistributedPeer.class);
		mockServiceToReturnPeers(failingPeer);
		when(requestOnSearchPeer.executeRequest(failingPeer)).thenThrow(
				new RuntimeException());
		List<JSONObject> actual = requestOnSearchPeers.execute();
		assertEquals(actual, new ArrayList<DistributedPeer>());
	}

	public void execute_onePeerThrowsAnotherSucceeds_getsSucceedingPeersJson() {
		DistributedPeer failingPeer = mock(DistributedPeer.class);
		DistributedPeer successfulPeer = mock(DistributedPeer.class);
		JSONObject successfulJson = new JSONObject();

		mockServiceToReturnPeers(failingPeer, successfulPeer);
		when(requestOnSearchPeer.executeRequest(failingPeer)).thenThrow(
				new RuntimeException());
		when(requestOnSearchPeer.executeRequest(successfulPeer)).thenReturn(
				successfulJson);

		List<JSONObject> actual = requestOnSearchPeers.execute();
		assertEquals(actual.size(), 1);
		assertTrue(actual.get(0) == successfulJson);
	}

	public void getExceptions_noPeers_noExceptions() {
		mockServiceToReturnPeers();
		requestOnSearchPeers.execute();
		List<RuntimeException> actual = requestOnSearchPeers.getExceptions();
		assertEquals(actual, new ArrayList<RuntimeException>());
	}

	public void getExceptions_onePeerThatThrows_canGetException() {
		mockServiceToReturnPeers(mock(DistributedPeer.class));
		RuntimeException exception = new RuntimeException();
		when(requestOnSearchPeer.executeRequest(any(DistributedPeer.class)))
				.thenThrow(exception);
		requestOnSearchPeers.execute();
		List<RuntimeException> exceptions = requestOnSearchPeers.getExceptions();
		assertEquals(exceptions.size(), 1);
		assertEquals(exceptions.get(0), exception);
	}

	public void getExceptions_onePeerThatThrows_exceptionsAreClearedEveryExecute() {
		mockServiceToReturnPeers(mock(DistributedPeer.class));
		RuntimeException exception = new RuntimeException();
		when(requestOnSearchPeer.executeRequest(any(DistributedPeer.class)))
				.thenThrow(exception);
		requestOnSearchPeers.execute();
		requestOnSearchPeers.execute();
		List<RuntimeException> exceptions = requestOnSearchPeers.getExceptions();
		assertEquals(exceptions.size(), 1);
		assertEquals(exceptions.get(0), exception);
	}
}
