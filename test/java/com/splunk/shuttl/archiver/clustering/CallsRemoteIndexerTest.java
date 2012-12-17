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
package com.splunk.shuttl.archiver.clustering;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class CallsRemoteIndexerTest {

	private CallsRemoteIndexer callsRemoteIndexer;
	private ShuttlPortEndpointProvider entityProvider;
	private RemoteShuttl remoteShuttl;

	@BeforeMethod
	public void setUp() {
		remoteShuttl = mock(RemoteShuttl.class);
		entityProvider = mock(ShuttlPortEndpointProvider.class);
		callsRemoteIndexer = new CallsRemoteIndexer(entityProvider, remoteShuttl);
	}

	public void _givenRemoteService_callsShuttlPortEndpoint() {
		int shuttlPort = 1234;
		String expectedName = "serverName";

		IndexerInfo indexerInfo = mock(IndexerInfo.class);
		when(indexerInfo.getHost()).thenReturn("host");

		ShuttlPortEndpoint shuttlService = mock(ShuttlPortEndpoint.class);
		when(entityProvider.getForIndexerInfo(indexerInfo)).thenReturn(
				shuttlService);
		when(shuttlService.getShuttlPort()).thenReturn(shuttlPort);
		when(remoteShuttl.getServerName(indexerInfo.getHost(), shuttlPort))
				.thenReturn(expectedName);

		String actualName = callsRemoteIndexer
				.getShuttlConfiguredServerName(indexerInfo);
		assertEquals(actualName, expectedName);
	}
}
