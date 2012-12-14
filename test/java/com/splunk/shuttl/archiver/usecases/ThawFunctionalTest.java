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
package com.splunk.shuttl.archiver.usecases;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static com.splunk.shuttl.testutil.TUtilsFunctional.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.util.Date;

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
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.thaw.BucketThawer;
import com.splunk.shuttl.archiver.thaw.BucketThawerFactory;
import com.splunk.shuttl.archiver.thaw.SplunkIndexesLayer;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsDate;
import com.splunk.shuttl.testutil.TUtilsFile;

@Test(groups = { "functional" })
public class ThawFunctionalTest {

	private String thawIndex;
	private BucketArchiver bucketArchiver;
	private ArchiveFileSystem archiveFileSystem;
	private File thawDirectory;
	private BucketThawer bucketThawer;
	private ArchiveConfiguration config;
	private File archiverData;

	@BeforeMethod
	public void setUp() throws IllegalIndexException {
		thawIndex = "someIndex";
		config = getLocalFileSystemConfiguration();
		archiveFileSystem = ArchiveFileSystemFactory.getWithConfiguration(config);
		archiverData = createDirectory();
		LocalFileSystemPaths localFileSystemPaths = new LocalFileSystemPaths(
				archiverData.getAbsolutePath());
		bucketArchiver = BucketShuttlerFactory
				.createWithConfFileSystemAndLocalPaths(config, archiveFileSystem,
						localFileSystemPaths);
		thawDirectory = TUtilsFile.createDirectory();

		SplunkIndexesLayer splunkIndexesLayer = mock(SplunkIndexesLayer.class);
		when(splunkIndexesLayer.getThawLocation(thawIndex)).thenReturn(
				thawDirectory);

		bucketThawer = BucketThawerFactory
				.createWithConfigAndSplunkSettingsAndLocalFileSystemPaths(config,
						splunkIndexesLayer, localFileSystemPaths);
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(thawDirectory);
		FileUtils.deleteQuietly(archiverData);
		tearDownLocalConfig(config);
	}

	public void Thawer_givenOneArchivedBucket_thawArchivedBucket() {
		Date earliest = TUtilsDate.getNowWithoutMillis();
		Date latest = earliest;
		LocalBucket bucket = TUtilsBucket.createBucketWithIndexAndTimeRange(
				thawIndex, earliest, latest);
		archiveBucket(bucket, bucketArchiver);

		assertTrue(isDirectoryEmpty(thawDirectory));
		bucketThawer.thawBuckets(thawIndex, earliest, latest);
		assertExactlyOneDirectoryInThawDirectory();
		String thawedName = thawDirectory.listFiles()[0].getName();
		assertEquals(bucket.getName(), thawedName);
	}

	private static final int HUNDRED_SECONDS = 100000;
	private static final int SECOND = 1000;

	public void Thawer_archivingBucketsInThreeDifferentTimeRanges_filterByOnlyOneOfTheTimeRanges()
			throws Exception {
		Date earliest = TUtilsDate.getNowWithoutMillis();
		Date latest = TUtilsDate.getLaterDate(earliest);
		for (int i = 0; i < 3; i++) {
			Date early = new Date(earliest.getTime() + i * HUNDRED_SECONDS);
			Date later = new Date(early.getTime() + SECOND);
			LocalBucket bucket = TUtilsBucket.createBucketWithIndexAndTimeRange(
					thawIndex, early, later);
			archiveBucket(bucket, bucketArchiver);
			assertFalse(bucket.getDirectory().exists());
		}
		assertTrue(isDirectoryEmpty(thawDirectory));

		bucketThawer.thawBuckets(thawIndex, earliest, latest);
		assertExactlyOneDirectoryInThawDirectory();
	}

	private void assertExactlyOneDirectoryInThawDirectory() {
		File[] filesThawed = thawDirectory.listFiles();
		int bucketsInThawLocation = filesThawed.length;
		assertEquals(1, bucketsInThawLocation);
		assertTrue(filesThawed[0].isDirectory());
	}
}
