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
package com.splunk.shuttl.archiver.functional;

import static com.splunk.shuttl.archiver.functional.UtilsFunctional.*;
import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketArchiverFactory;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.thaw.BucketThawer;
import com.splunk.shuttl.archiver.thaw.BucketThawerFactory;
import com.splunk.shuttl.archiver.thaw.SplunkSettings;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFile;

@Test(groups = { "functional" })
public class ThawFunctionalTest {

    private String thawIndex;
    private BucketArchiver bucketArchiver;
    private ArchiveFileSystem archiveFileSystem;
    private File thawDirectory;
    private BucketThawer bucketThawer;
    private ArchiveConfiguration config;

    @BeforeMethod
    public void setUp() throws IllegalIndexException {
	thawIndex = "someIndex";
	config = getLocalFileSystemConfiguration();
	archiveFileSystem = ArchiveFileSystemFactory
		.getWithConfiguration(config);
	bucketArchiver = BucketArchiverFactory
		.createWithConfigurationAndArchiveFileSystem(config,
			archiveFileSystem);
	thawDirectory = TUtilsFile.createTempDirectory();

	SplunkSettings splunkSettings = mock(SplunkSettings.class);
	when(splunkSettings.getThawLocation(thawIndex)).thenReturn(
		thawDirectory);
	bucketThawer = BucketThawerFactory.createWithSplunkSettingsAndConfig(
		splunkSettings, config);
    }

    @AfterMethod
    public void tearDown() {
	FileUtils.deleteQuietly(thawDirectory);
	tearDownLocalConfig(config);
    }

    public void Thawer_givenOneArchivedBucket_thawArchivedBucket() {
	Date earliest = new Date();
	Date latest = earliest;
	Bucket bucket = TUtilsBucket.createBucketWithIndexAndTimeRange(
		thawIndex, earliest, latest);
	archiveBucket(bucket, bucketArchiver);

	assertTrue(isDirectoryEmpty(thawDirectory));
	bucketThawer.thawBuckets(thawIndex, earliest, latest);
	assertExactlyOneDirectoryInThawDirectory();
	String thawedName = thawDirectory.listFiles()[0].getName();
	assertEquals(bucket.getName(), thawedName);
    }

    public void Thawer_archivingBucketsInThreeDifferentTimeRanges_filterByOnlyOneOfTheTimeRanges()
	    throws Exception {
	Date earliest = new Date(1332295013);
	Date latest = new Date(earliest.getTime() + 26);
	for (int i = 0; i < 3; i++) {
	    Date early = new Date(earliest.getTime() + i * 100);
	    Date later = new Date(early.getTime() + 30);
	    Bucket bucket = TUtilsBucket.createBucketWithIndexAndTimeRange(
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
