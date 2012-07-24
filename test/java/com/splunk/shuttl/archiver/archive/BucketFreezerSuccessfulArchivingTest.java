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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.ClientProtocolException;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.recovery.IndexPreservingBucketMover;
import com.splunk.shuttl.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shuttl.archiver.bucketlock.BucketLocker.SharedLockBucketHandler;
import com.splunk.shuttl.archiver.bucketlock.BucketLockerInTestDir;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsTestNG;

/**
 * Fixture: HttpClient returns HttpStatus codes that represent successful
 * archiving.
 */
@Test(groups = { "fast-unit" })
public class BucketFreezerSuccessfulArchivingTest {

	File tempTestDirectory;
	BucketFreezer bucketFreezer;
	ArchiveRestHandler archiveRestHandler;
	FailedBucketsArchiver failedBucketsArchiver;

	@BeforeMethod
	public void setUp() throws ClientProtocolException, IOException {
		tempTestDirectory = createDirectory();
		archiveRestHandler = mock(ArchiveRestHandler.class);
		failedBucketsArchiver = mock(FailedBucketsArchiver.class);
		bucketFreezer = new BucketFreezer(IndexPreservingBucketMover.create(tempTestDirectory),
				new BucketLockerInTestDir(tempTestDirectory), archiveRestHandler,
				failedBucketsArchiver);
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(tempTestDirectory);
	}

	@Test(groups = { "fast-unit" })
	public void should_moveDirectoryToaSafeLocation_when_givenPath()
			throws IOException {
		File dirToBeMoved = createDirectory();
		File safeLocationDirectory = tempTestDirectory;
		assertTrue(isDirectoryEmpty(safeLocationDirectory));

		// Test
		int exitStatus = bucketFreezer.freezeBucket("index-name",
				dirToBeMoved.getAbsolutePath());
		assertEquals(0, exitStatus);

		// Verify
		assertTrue(!dirToBeMoved.exists());
		assertTrue(safeLocationDirectory.exists());
		assertTrue(!isDirectoryEmpty(safeLocationDirectory));
	}

	public void freezeBucket_givenNonExistingSafeLocation_createSafeLocation()
			throws IOException {
		File dirToBeMoved = createDirectory();
		System.err.println(tempTestDirectory);
		assertTrue(FileUtils.deleteQuietly(tempTestDirectory));
		File nonExistingSafeLocation = tempTestDirectory;
		assertTrue(!nonExistingSafeLocation.exists());
		System.err.println(tempTestDirectory.getName());

		// Test
		bucketFreezer.freezeBucket("index", dirToBeMoved.getAbsolutePath());

		// Verify
		assertTrue(!dirToBeMoved.exists());
		assertTrue(nonExistingSafeLocation.exists());
	}

	public void freezeBucket_givenBucket_callRestWithMovedBucket() {
		Bucket bucket = TUtilsBucket.createBucket();

		bucketFreezer.freezeBucket(bucket.getIndex(), bucket.getDirectory()
				.getAbsolutePath());

		ArgumentCaptor<Bucket> bucketCaptor = ArgumentCaptor.forClass(Bucket.class);
		verify(archiveRestHandler, times(1)).callRestToArchiveBucket(
				bucketCaptor.capture());
		assertEquals(1, bucketCaptor.getAllValues().size());
		Bucket capturedBucket = bucketCaptor.getValue();

		TUtilsTestNG.assertBucketsGotSameIndexFormatAndName(bucket, capturedBucket);
	}

	public void freezeBucket_givenBucket_callItToRecoverBuckets() {
		Bucket bucket = TUtilsBucket.createBucket();
		bucketFreezer.freezeBucket(bucket.getIndex(), bucket.getDirectory()
				.getAbsolutePath());
		verify(failedBucketsArchiver).archiveFailedBuckets(archiveRestHandler);
	}

	public void freezeBucket_givenBucket_triesToRestoreBucketsAFTERCallingRest() {
		Bucket bucket = TUtilsBucket.createBucket();
		bucketFreezer.freezeBucket(bucket.getIndex(), bucket.getDirectory()
				.getAbsolutePath());
		InOrder inOrder = inOrder(archiveRestHandler, failedBucketsArchiver);
		inOrder.verify(archiveRestHandler, times(1)).callRestToArchiveBucket(
				any(Bucket.class));
		inOrder.verify(failedBucketsArchiver).archiveFailedBuckets(
				any(SharedLockBucketHandler.class));
		inOrder.verifyNoMoreInteractions();
	}
}
