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

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

/**
 * Fixture: Makes sure that {@link ArchiveRestHandler} calls rest as recovery.
 */
@Test(groups = { "fast-unit" })
public class ArchiveRestHandlerTest {

	@Test(groups = { "fast-unit" })
	public void callRestToArchiveBucket_givenBucket_executesRequestOnHttpClient()
			throws ClientProtocolException, IOException {
		Bucket bucket = TUtilsBucket.createBucket();
		HttpClient httpClient = mock(HttpClient.class, Mockito.RETURNS_MOCKS);
		ArchiveRestHandler archiveRestHandler = new ArchiveRestHandler(httpClient);
		archiveRestHandler.callRestToArchiveBucket(bucket);

		verify(httpClient).execute(any(HttpUriRequest.class));
	}
}
