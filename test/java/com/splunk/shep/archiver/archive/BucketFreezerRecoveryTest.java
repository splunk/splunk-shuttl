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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.recovery.FailedBucketLock;
import com.splunk.shep.archiver.archive.recovery.FailedBucketRecoveryHandler;
import com.splunk.shep.archiver.archive.recovery.FailedBucketRestorer;
import com.splunk.shep.archiver.archive.recovery.FailedBucketTransfers;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsFile;
import com.splunk.shep.testutil.UtilsMockito;

/**
 * Fixture: Created with recovery classes for re-archiving failed buckets.
 */
@Test(groups = { "fast" })
public class BucketFreezerRecoveryTest {

    File safeLocationForBuckets;
    BucketFreezer bucketFreezer;
    FailedBucketTransfers failedBucketTransfers;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	safeLocationForBuckets = UtilsFile.createTempDirectory();
	failedBucketTransfers = mock(FailedBucketTransfers.class);
	ArchiveRestHandler archiveRestHandler = new ArchiveRestHandler(null,
		failedBucketTransfers);
	bucketFreezer = new BucketFreezer(
		safeLocationForBuckets.getAbsolutePath(), archiveRestHandler,
		null);
    }

    @AfterMethod(groups = { "fast" })
    public void tearDown() {
	FileUtils.deleteQuietly(safeLocationForBuckets);
    }

    public void freezeBucket_givenRandomHttpClientAndFailedBucketRestorer_triesToRestoreBucketsAFTERCallingHttpClient()
	    throws ClientProtocolException, IOException {
	FailedBucketRestorer failedBucketRestorer = mock(FailedBucketRestorer.class);
	bucketFreezer.failedBucketRestorer = failedBucketRestorer;
	HttpClient httpClient = UtilsMockito.createRandomHttpStatusHttpClient();
	bucketFreezer.setHttpClient(httpClient);
	Bucket bucket = UtilsBucket.createTestBucket();
	bucketFreezer.freezeBucket(bucket.getIndex(), bucket.getDirectory()
		.getAbsolutePath());
	// Verify that recover happens after the HttpClient call.
	InOrder inOrder = inOrder(httpClient, failedBucketRestorer);
	inOrder.verify(httpClient, times(1)).execute(any(HttpUriRequest.class));
	inOrder.verify(failedBucketRestorer).recoverFailedBuckets(
		any(FailedBucketRecoveryHandler.class));
    }

    public void freezeBucket_givenOneFailedBucket_callsHttpClientWithBucketToFreezeAndFailedBucket()
	    throws ClientProtocolException, IOException {
	FailedBucketLock failedBucketLock = mock(FailedBucketLock.class);
	bucketFreezer.failedBucketRestorer = new FailedBucketRestorer(
		failedBucketTransfers, failedBucketLock);

	HttpClient httpClient = mock(HttpClient.class);
	bucketFreezer.setHttpClient(httpClient);

	Bucket bucketToFreeze = UtilsBucket.createTestBucket();
	Bucket failedBucket = UtilsBucket.createTestBucket();

	// Setup the real implementation of FailedBucketRestorer
	when(failedBucketTransfers.getFailedBuckets()).thenReturn(
		Arrays.asList(failedBucket));
	when(failedBucketLock.tryLock()).thenReturn(true);

	// Test
	bucketFreezer.freezeBucket(bucketToFreeze.getIndex(), bucketToFreeze
		.getDirectory().getAbsolutePath());

	// Verify that both buckets were executed in http client.
	ArgumentCaptor<HttpUriRequest> requestCaptor = ArgumentCaptor
		.forClass(HttpUriRequest.class);
	verify(httpClient, times(2)).execute(requestCaptor.capture());
	List<HttpUriRequest> requests = requestCaptor.getAllValues();
	assertEquals(2, requests.size());
	for (HttpUriRequest request : requests) {
	    String uri = request.getURI().toString();
	    assertTrue(uri.contains(bucketToFreeze.getIndex())
		    || uri.contains(failedBucket.getIndex()));
	}
    }
}
