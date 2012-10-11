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
package com.splunk.shuttl.archiver.archive;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

/**
 * Fixture: Makes sure that {@link ArchiveRestHandler} calls rest as recovery.
 */
@Test(groups = { "fast-unit" })
public class ArchiveRestHandlerTest {

	private ArchiveRestHandler archiveRestHandler;
	private HttpClient httpClient;
	private Logger logger;

	@BeforeMethod
	public void setUp() {
		httpClient = mock(HttpClient.class, Mockito.RETURNS_MOCKS);
		logger = mock(Logger.class);
		archiveRestHandler = new ArchiveRestHandler(httpClient, logger);
	}

	@Test(groups = { "fast-unit" })
	public void callRestToArchiveLocalBucket_givenBucket_executesRequestOnHttpClient()
			throws ClientProtocolException, IOException {
		LocalBucket bucket = TUtilsBucket.createBucket();
		archiveRestHandler.callRestToArchiveLocalBucket(bucket);

		verify(httpClient).execute(any(HttpUriRequest.class));
	}

	@SuppressWarnings("unchecked")
	public void callRestToArchiveLocalBucket_httpClientThrowsClientProtocolException_caughtAndLogged()
			throws ClientProtocolException, IOException {
		when(httpClient.execute(any(HttpUriRequest.class))).thenThrow(
				ClientProtocolException.class);
		LocalBucket bucket = TUtilsBucket.createBucket();
		archiveRestHandler.callRestToArchiveLocalBucket(bucket);

		verifyClassWasOnlyErrorLogged(ClientProtocolException.class);
	}

	private void verifyClassWasOnlyErrorLogged(Class<?> clazz) {
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(logger).error(captor.capture());
		List<String> allErrorLogs = captor.getAllValues();
		assertEquals(1, allErrorLogs.size());
		assertTrue(allErrorLogs.get(0).contains(clazz.getSimpleName()));
	}

	@SuppressWarnings("unchecked")
	public void callRestToArchiveLocalBucket_httpClientThrowsIOException_caughtAndLogged()
			throws ClientProtocolException, IOException {
		when(httpClient.execute(any(HttpUriRequest.class))).thenThrow(
				IOException.class);
		LocalBucket bucket = TUtilsBucket.createBucket();
		archiveRestHandler.callRestToArchiveLocalBucket(bucket);

		verifyClassWasOnlyErrorLogged(IOException.class);
	}
}
