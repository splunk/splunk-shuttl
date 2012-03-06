package com.splunk.shep.testutil;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicHttpResponse;

/**
 * All the utils regarding Mockito goes in here. If there are exceptions while
 * doing any operations the tests will fail with appropriate message.
 */
public class UtilsMockito {

    /**
     * @return an HttpCLient that always returns HTTP OK coded responses when
     *         the execute code is called.
     */
    public static HttpClient createAlwaysOKReturningHTTPClientMock() {
	return createHttpClientMockReturningHttpStatus(HttpStatus.SC_OK);
    }

    /**
     * @return an HttpCLient that always returns HTTP OK coded responses when
     *         the execute code is called.
     */
    public static HttpClient createInternalServerErrorHttpClientMock() {
	return createHttpClientMockReturningHttpStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR);
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
}
