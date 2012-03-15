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

import static com.splunk.shep.testutil.UtilsFile.createTempDirectory;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.recovery.BucketMover;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsMockito;
import com.splunk.shep.testutil.UtilsTestNG;

/**
 * Fixture: BucketFreezer gets HttpStatus codes from HttpClient that means that
 * archiving bucket failed.
 */
@Test(groups = { "fast" })
public class ArchiveRestHandlerFailiureArchivingTest {

    File tempTestDirectory;
    BucketMover bucketMover;
    Bucket failedBucket;

    @BeforeMethod(groups = { "fast" })
    public void setUp_internalServerErrorHttpClientBucketFreezer() {
	tempTestDirectory = createTempDirectory();
	bucketMover = mock(BucketMover.class);
	failedBucket = UtilsBucket.createBucketInDirectory(tempTestDirectory);
    }

    @AfterMethod(groups = { "fast" })
    public void tearDown() {
	FileUtils.deleteQuietly(tempTestDirectory);
    }

    @Test(groups = { "fast" })
    public void freezeBucket_httpClientThrowsIOException_moveBucketToFailedLocation()
	    throws ClientProtocolException, IOException {
	freezeBucketAndLetHttpClientThrowsException(new IOException());
    }

    @Test(groups = { "fast" })
    public void freezeBucket_httpClientThrowsClientProtocolException_moveBucketToFailedLocation()
	    throws ClientProtocolException, IOException {
	freezeBucketAndLetHttpClientThrowsException(new ClientProtocolException());
    }

    private void freezeBucketAndLetHttpClientThrowsException(Exception exception)
	    throws IOException, ClientProtocolException {
	HttpClient exceptionThrowingHttpClient = mock(HttpClient.class);
	when(exceptionThrowingHttpClient.execute(any(HttpUriRequest.class)))
		.thenThrow(exception);

	new ArchiveRestHandler(exceptionThrowingHttpClient,
		bucketMover).callRestToArchiveBucket(failedBucket);

	verifyFailedBucketTransfersWasCalledWithBucket(failedBucket);
    }

    private void verifyFailedBucketTransfersWasCalledWithBucket(Bucket bucket) {
	ArgumentCaptor<Bucket> bucketCaptor = ArgumentCaptor
		.forClass(Bucket.class);

	verify(bucketMover, times(1)).moveFailedBucket(
		bucketCaptor.capture());
	Bucket capturedBucket = bucketCaptor.getValue();
	UtilsTestNG.assertBucketsGotSameIndexFormatAndName(bucket,
		capturedBucket);
    }

    @Test(groups = { "fast" })
    public void freezeBucket_internalServerError_moveBucketWithFailedBucketTransfers()
	    throws IOException {
	HttpClient failingHttpClient = UtilsMockito
		.createInternalServerErrorHttpClientMock();

	new ArchiveRestHandler(failingHttpClient, bucketMover)
		.callRestToArchiveBucket(failedBucket);

	// Verification
	verifyFailedBucketTransfersWasCalledWithBucket(failedBucket);
    }

}
