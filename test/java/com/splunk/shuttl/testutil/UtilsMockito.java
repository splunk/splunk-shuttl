// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.testutil;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Random;

import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicHttpResponse;

import com.splunk.EntityCollection;
import com.splunk.Index;
import com.splunk.Service;

/**
 * All the utils regarding Mockito goes in here. If there are exceptions while
 * doing any operations the tests will fail with appropriate message.
 */
public class UtilsMockito {

    /**
     * @return an HttpClient that always returns HTTP OK coded responses when
     *         the execute code is called.
     */
    public static HttpClient createAlwaysOKReturningHTTPClientMock() {
	return createHttpClientMockReturningHttpStatus(HttpStatus.SC_OK);
    }

    /**
     * @return an HttpClient that always returns HttpStatus Internal Error (500)
     */
    public static HttpClient createInternalServerErrorHttpClientMock() {
	return createHttpClientMockReturningHttpStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    /**
     * @return HttpClient that always random HttpStatus.
     */
    public static HttpClient createRandomHttpStatusHttpClient() {
	HttpClient httpClient = mock(HttpClient.class);
	StatusLine statusLine = mock(StatusLine.class);
	try {
	    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(
		    new BasicHttpResponse(statusLine));
	} catch (Exception e) {
	    UtilsTestNG.failForException(
		    "Could not assign return value for execute", e);
	}
	when(statusLine.getStatusCode()).thenReturn(getAnyHttpStatus());
	return httpClient;
    }

    /**
     * @return a fairly random HttpStatus
     */
    private static Integer getAnyHttpStatus() {
	Integer[] someHttpStatuses = new Integer[] { HttpStatus.SC_ACCEPTED,
		HttpStatus.SC_GATEWAY_TIMEOUT, HttpStatus.SC_OK,
		HttpStatus.SC_INTERNAL_SERVER_ERROR };
	Random rand = new Random();
	return someHttpStatuses[rand.nextInt(someHttpStatuses.length)];
    }

    private static HttpClient createHttpClientMockReturningHttpStatus(
	    int httpStatus) {
	HttpClient httpClient = mock(HttpClient.class);
	StatusLine statusLine = mock(StatusLine.class);
	when(statusLine.getStatusCode()).thenReturn(httpStatus);
	try {
	    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(
		    new BasicHttpResponse(statusLine));
	} catch (Exception e) {
	    UtilsTestNG.failForException(
		    "Couldn't assign return value for execute", e);
	}
	return httpClient;
    }

    /**
     * @return {@link Service} that returns a thawLocationPath for indexName.
     */
    @SuppressWarnings("unchecked")
    public static Service createSplunkServiceReturningThawPathForIndex(
	    String indexName, String thawLocationPath) {
	Service splunkService = mock(Service.class);
	EntityCollection<Index> indexesMock = mock(EntityCollection.class);
	Index indexMock = mock(Index.class);

	when(splunkService.getIndexes()).thenReturn(indexesMock);
	when(indexesMock.get(indexName)).thenReturn(indexMock);
	when(indexMock.getThawedPathExpanded()).thenReturn(thawLocationPath);
	return splunkService;
    }
}
