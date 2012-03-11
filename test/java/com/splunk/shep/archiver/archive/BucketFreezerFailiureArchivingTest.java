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

import static com.splunk.shep.testutil.UtilsFile.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

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

import com.splunk.shep.archiver.archive.recovery.FailedBucketRestorer;
import com.splunk.shep.archiver.archive.recovery.FailedBucketTransfers;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsMockito;

/**
 * Fixture: BucketFreezer gets HttpStatus codes from HttpClient that means that
 * archiving bucket failed.
 */
@Test(groups = { "fast" })
public class BucketFreezerFailiureArchivingTest {

    BucketFreezer bucketFreezer;
    File failedBucketsLocation;
    File safeLocation;
    FailedBucketTransfers failedBucketTransfers;

    @BeforeMethod(groups = { "fast" })
    public void setUp_internalServerErrorHttpClientBucketFreezer() {
	safeLocation = createTempDirectory();
	failedBucketTransfers = mock(FailedBucketTransfers.class);
	failedBucketsLocation = createTempDirectory();
	HttpClient failingHttpClient = UtilsMockito
		.createInternalServerErrorHttpClientMock();
	bucketFreezer = new BucketFreezer(safeLocation.getAbsolutePath(),
		failingHttpClient, failedBucketTransfers,
		mock(FailedBucketRestorer.class));
    }

    @AfterMethod(groups = { "fast" })
    public void tearDown() {
	FileUtils.deleteQuietly(failedBucketsLocation);
	FileUtils.deleteQuietly(safeLocation);
    }

    public void freezeBucket_internalServerError_moveBucketWithFailedBucketTransfers()
	    throws IOException {
	Bucket failedBucket = UtilsBucket.createTestBucket();
	bucketFreezer.freezeBucket(failedBucket.getIndex(), failedBucket
		.getDirectory().getAbsolutePath());

	// Verification
	verifyFailedBucketTransfersWasCalledWithBucket(failedBucket);
    }

    private void verifyFailedBucketTransfersWasCalledWithBucket(Bucket bucket) {
	ArgumentCaptor<Bucket> bucketCaptor = ArgumentCaptor
		.forClass(Bucket.class);

	verify(failedBucketTransfers, times(1)).moveFailedBucket(
		bucketCaptor.capture());
	Bucket capturedBucket = bucketCaptor.getValue();
	assertEquals(bucket.getIndex(), capturedBucket.getIndex());
	assertEquals(bucket.getName(), capturedBucket.getName());
	assertEquals(bucket.getFormat(), capturedBucket.getFormat());
    }

    public void freezeBucket_httpClientThrowsIOException_moveBucketToFailedLocation()
	    throws ClientProtocolException, IOException {
	freezeBucketAndLetHttpClientThrowsException(new IOException());
    }

    private void freezeBucketAndLetHttpClientThrowsException(Exception exception)
	    throws IOException, ClientProtocolException {
	Bucket bucket = UtilsBucket.createTestBucket();
	HttpClient exceptionThrowingHttpClient = mock(HttpClient.class);
	when(exceptionThrowingHttpClient.execute(any(HttpUriRequest.class)))
		.thenThrow(exception);

	bucketFreezer.httpClient = exceptionThrowingHttpClient;
	bucketFreezer.freezeBucket(bucket.getIndex(), bucket.getDirectory()
		.getAbsolutePath());

	verifyFailedBucketTransfersWasCalledWithBucket(bucket);
    }

    public void freezeBucket_httpClientThrowsClientProtocolException_moveBucketToFailedLocation()
	    throws ClientProtocolException, IOException {
	freezeBucketAndLetHttpClientThrowsException(new ClientProtocolException());
    }

}
