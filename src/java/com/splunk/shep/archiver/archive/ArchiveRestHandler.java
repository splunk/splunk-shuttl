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
package com.splunk.shep.archiver.archive;

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.splunk.shep.archiver.archive.recovery.BucketLocker.LockedBucketHandler;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.server.mbeans.rest.BucketArchiverRest;

/**
 * Handling all the calls and returns to and from {@link BucketArchiverRest}
 */
public class ArchiveRestHandler implements LockedBucketHandler {

    private final static Logger logger = Logger
	    .getLogger(ArchiveRestHandler.class);
    private final HttpClient httpClient;

    /**
     * TODO:
     */
    public ArchiveRestHandler(HttpClient httpClient) {
	this.httpClient = httpClient;
    }

    public void callRestToArchiveBucket(Bucket bucket) {
	HttpUriRequest archiveBucketRequest = createBucketArchiveRequest(bucket);
	HttpResponse response = null;
	try {
	    if(logger.isDebugEnabled()) {
		logger.debug(will("Send an archive bucket request",
			"request_uri",
		    archiveBucketRequest.getURI()));
	    }
	    response = httpClient.execute(archiveBucketRequest); // LOG
	    if (response != null) {
		handleResponseCodeFromDoingArchiveBucketRequest(response
			.getStatusLine().getStatusCode(), bucket);
	    } else {
		// LOG: warning! Response was null. This happens in our tests
		// when we mock the httpClient. Should never happen other wise.
		// Should it?
	    }
	} catch (ClientProtocolException e) {
	    handleIOExceptionGenereratedByDoingArchiveBucketRequest(e, bucket);
	} catch (IOException e) {
	    handleIOExceptionGenereratedByDoingArchiveBucketRequest(e, bucket);
	} finally {
	    consumeResponseHandlingErrors(response);
	}
    }

    private void consumeResponseHandlingErrors(HttpResponse response) {
	if (response != null) {
	    try {
		EntityUtils.consume(response.getEntity());
	    } catch (IOException e) {
		if(logger.isDebugEnabled()) {
		    logger.debug(did(
			    "Tried to consume http response of archive bucket request",
			e, "no exception", "response", response));
		}
	    }
	}
    }

    private void handleResponseCodeFromDoingArchiveBucketRequest(
	    int statusCode, Bucket bucket) {
	// TODO handle the different status codes
	switch (statusCode) {
	case HttpStatus.SC_OK:
	case HttpStatus.SC_NO_CONTENT:
	    if(logger.isDebugEnabled()) {
		logger.debug(done(
			"Got http response from archiveBucketRequest",
			"status_code", statusCode));
	    }
	    break;
	default:
	    if(logger.isDebugEnabled()) {
		logger.debug(did("Sent an archive bucket reuqest",
			"Got non ok http_status",
		    "expected HttpStatus.SC_OK or SC_NO_CONTENT",
		    "http_status", statusCode));
	    }
	}
    }

    private void handleIOExceptionGenereratedByDoingArchiveBucketRequest(
	    IOException e, Bucket bucket) {
	if(logger.isDebugEnabled()) {
	    logger.debug(did("Archive bucket request", "got IOException",
		    "request to succeed", "exception", e));
	}
	// TODO this method should handle the errors in case the bucket transfer
	// fails. In this state there is no way of telling if the bucket was
	// actually transfered or not.
    }

    private HttpUriRequest createBucketArchiveRequest(Bucket bucket) {
	// CONFIG configure the host, port, request URL with a general
	// solution.
	String requestString = "http://localhost:9090/shep/rest/archiver/bucket/archive?path="
		+ bucket.getDirectory().getAbsolutePath()
		+ "&index="
		+ bucket.getIndex();
	HttpGet request = new HttpGet(requestString);
	return request;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.archiver.archive.recovery.BucketLocker.LockedBucketHandler
     * #handleLockedBucket(com.splunk.shep.archiver.model.Bucket)
     */
    @Override
    public void handleLockedBucket(Bucket bucket) {
	callRestToArchiveBucket(bucket);
    }

}
