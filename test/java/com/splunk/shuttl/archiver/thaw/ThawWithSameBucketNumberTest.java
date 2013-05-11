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
package com.splunk.shuttl.archiver.thaw;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketShuttlerFactory;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFunctional;

@Test(groups = { "functional" }, enabled = false)
public class ThawWithSameBucketNumberTest {

	private ArchiveConfiguration config;
	private File archiverData;
	private BucketArchiver bucketArchiver;
	private ArchiveFileSystem archiveFileSystem;
	private LocalFileSystemPaths localFileSystemPaths;
	private SplunkIndexesLayer splunkIndexesLayer;
	private BucketThawer bucketThawer;

	@BeforeMethod
	public void setUp() {
		config = TUtilsFunctional.getLocalFileSystemConfiguration();
		archiveFileSystem = ArchiveFileSystemFactory.getWithConfiguration(config);
		archiverData = createDirectory();
		localFileSystemPaths = new LocalFileSystemPaths(
				archiverData.getAbsolutePath());
		bucketArchiver = BucketShuttlerFactory
				.createWithConfFileSystemAndLocalPaths(config, archiveFileSystem,
						localFileSystemPaths);

		File thawDir = createDirectory();
		splunkIndexesLayer = mock(SplunkIndexesLayer.class);
		when(splunkIndexesLayer.getThawLocation(anyString())).thenReturn(thawDir);
		bucketThawer = BucketThawerFactory.create(config, splunkIndexesLayer,
				localFileSystemPaths, archiveFileSystem);
	}

	@AfterMethod
	public void tearDown() {
		TUtilsFunctional.tearDownLocalConfig(config);
		FileUtils.deleteQuietly(archiverData);
	}

	public void _givenTwoBucketsArchivedWithSameBucketNumber_whenThawedBucketNumbersDifferFromEachOther() {
		int bucketNumber = 0;
		LocalBucket b1 = TUtilsBucket.createBucketWithBucketNumber(bucketNumber);
		LocalBucket b2 = TUtilsBucket.createBucketWithBucketNumber(bucketNumber,
				b1.getIndex(), new Date(b1.getLatest().getTime() + 1000),
				b1.getEarliest());
		assertNotEquals(b1.getName(), b2.getName());
		assertEquals(b1.getIndex(), b2.getIndex());
		assertEquals(b1.getBucketNumber(), b2.getBucketNumber());

		TUtilsFunctional.archiveBucket(b1, bucketArchiver);
		TUtilsFunctional.archiveBucket(b2, bucketArchiver);

		bucketThawer.thawBuckets(b1.getIndex(), b1.getEarliest(), b2.getLatest());
		List<LocalBucket> thawedBuckets = bucketThawer.getThawedBuckets();
		assertEquals(2, thawedBuckets.size());

		assertBucketNumbersOfThawedBucketsAreNotEqualToEachOther(b1, b2,
				thawedBuckets);
	}

	private void assertBucketNumbersOfThawedBucketsAreNotEqualToEachOther(
			LocalBucket b1, LocalBucket b2, List<LocalBucket> thawedBuckets) {
		LocalBucket thawedBucket1 = null, thawedBucket2 = null;

		for (LocalBucket b : thawedBuckets)
			if (b.getLatest().equals(b1.getLatest()))
				thawedBucket1 = b;
			else if (b.getLatest().equals(b2.getLatest()))
				thawedBucket2 = b;
			else
				throw new RuntimeException(
						"one of the bucket's should have the same latest time");

		assertNotNull(thawedBucket1);
		assertNotNull(thawedBucket2);

		assertOneOfTheThawedBucketsAreTheGivenBucketNumber(b1.getBucketNumber(),
				thawedBucket1, thawedBucket2);
		assertNotEquals(thawedBucket1.getBucketNumber(),
				thawedBucket2.getBucketNumber());
	}

	private void assertOneOfTheThawedBucketsAreTheGivenBucketNumber(
			long bucketNumber, LocalBucket tb1, LocalBucket tb2) {
		assertTrue(tb1.getBucketNumber() == bucketNumber
				|| tb2.getBucketNumber() == bucketNumber);
	}
}
