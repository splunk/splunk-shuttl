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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketShuttlerFactory;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.thaw.BucketThawer;
import com.splunk.shuttl.archiver.thaw.BucketThawerFactory;
import com.splunk.shuttl.archiver.thaw.SplunkSettings;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsFunctional;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "end-to-end" })
public class ImportCsvFunctionalTest {

	private ArchiveConfiguration localCsvArchiveConfigration;
	private BucketThawer csvThawer;
	private LocalBucket realBucket;
	private BucketArchiver csvArchiver;
	private File thawDirectory;
	private File archiverData;

	@BeforeMethod
	public void setUp() throws IllegalIndexException {
		localCsvArchiveConfigration = TUtilsFunctional
				.getLocalCsvArchiveConfigration();
		SplunkSettings splunkSettings = mock(SplunkSettings.class);
		thawDirectory = createDirectory();
		when(splunkSettings.getThawLocation(anyString())).thenReturn(thawDirectory);

		archiverData = createDirectory();
		LocalFileSystemPaths localFileSystemPaths = new LocalFileSystemPaths(
				archiverData.getAbsolutePath());

		csvThawer = BucketThawerFactory
				.createWithConfigAndSplunkSettingsAndLocalFileSystemPaths(
						localCsvArchiveConfigration, splunkSettings,
						localFileSystemPaths);

		realBucket = TUtilsBucket.createRealBucket();
		csvArchiver = BucketShuttlerFactory.createWithConfAndLocalPaths(
				localCsvArchiveConfigration, localFileSystemPaths);
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(thawDirectory);
		FileUtils.deleteQuietly(archiverData);
	}

	@Parameters(value = { "splunk.home" })
	public void _givenArchivedCsvBucket_thawedBucketEqualsArchivedRealBucket(
			final String splunkHome) throws FileNotFoundException,
			FileNotDirectoryException {
		final long sizeOfRealBucket = sizeOfBucket(realBucket);
		archiveBucketAsCsvWithExportToolThatNeedsSplunkHome(splunkHome);
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
				thawBucketAndComparePropertiesToRealBucket(sizeOfRealBucket);
			}

			private void thawBucketAndComparePropertiesToRealBucket(
					final long sizeOfRealBucket) {
				csvThawer.thawBuckets(realBucket.getIndex(), realBucket.getEarliest(),
						realBucket.getLatest());
				List<Bucket> thawedBuckets = csvThawer.getThawedBuckets();

				assertEquals(1, thawedBuckets.size());
				Bucket thawedBucket = thawedBuckets.get(0);
				TUtilsTestNG.assertBucketsGotSameIndexFormatAndName(realBucket,
						thawedBucket);
				assertEquals(sizeOfRealBucket, (long) thawedBucket.getSize());
			}
		});
	}

	private void archiveBucketAsCsvWithExportToolThatNeedsSplunkHome(
			String splunkHome) {
		TUtilsFunctional.archiveBucket(realBucket, csvArchiver, splunkHome);
	}

	private long sizeOfBucket(LocalBucket b) {
		return FileUtils.sizeOfDirectory(b.getDirectory());
	}
}
