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
package com.splunk.shep.archiver.functional;

import static com.splunk.shep.testutil.UtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.Service;
import com.splunk.shep.archiver.archive.ArchiveConfiguration;
import com.splunk.shep.archiver.archive.ArchiveRestHandler;
import com.splunk.shep.archiver.archive.BucketFreezer;
import com.splunk.shep.archiver.archive.PathResolver;
import com.splunk.shep.archiver.archive.recovery.BucketLock;
import com.splunk.shep.archiver.archive.recovery.BucketLocker;
import com.splunk.shep.archiver.archive.recovery.BucketMover;
import com.splunk.shep.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shep.archiver.fileSystem.HadoopFileSystemArchive;
import com.splunk.shep.archiver.listers.ArchiveBucketsLister;
import com.splunk.shep.archiver.listers.ArchivedIndexesLister;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;
import com.splunk.shep.archiver.thaw.BucketFilter;
import com.splunk.shep.archiver.thaw.BucketFormatChooser;
import com.splunk.shep.archiver.thaw.BucketFormatResolver;
import com.splunk.shep.archiver.thaw.BucketThawer;
import com.splunk.shep.archiver.thaw.SplunkSettings;
import com.splunk.shep.archiver.thaw.ThawBucketTransferer;
import com.splunk.shep.archiver.thaw.ThawLocationProvider;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsFile;
import com.splunk.shep.testutil.UtilsMockito;

@Test(enabled = false, groups = { "super-slow" })
public class ThawFunctionalTest {

    File tempDirectory;
    BucketFreezer successfulBucketFreezer;
    BucketThawer bucketThawer;
    String thawIndex;
    SplunkSettings splunkSettings;
    File thawDirectoryLocation;
    Path tmpPath;

    @BeforeMethod
    public void setUp() {
	thawIndex = "thawingIndex";
	tempDirectory = createTempDirectory();
	successfulBucketFreezer = getSuccessfulBucketFreezer();
	thawDirectoryLocation = createDirectoryInParent(tempDirectory,
		"thawDirectory");
	tmpPath = new Path("/tmp/" + RandomUtils.nextInt() + "/");

	PathResolver pathResolver = UtilsArchiverFunctional
		.getRealPathResolver();
	FileSystem hadoopFileSystem = UtilsArchiverFunctional
		.getHadoopFileSystem();
	HadoopFileSystemArchive archiveFileSystem = new HadoopFileSystemArchive(
		hadoopFileSystem, tmpPath);
	ArchivedIndexesLister indexesLister = new ArchivedIndexesLister(
		pathResolver, archiveFileSystem);
	ArchiveBucketsLister bucketsLister = new ArchiveBucketsLister(
		archiveFileSystem, indexesLister, pathResolver);
	BucketFilter bucketFilter = new BucketFilter();
	BucketFormatChooser bucketFormatChooser = new BucketFormatChooser(
		ArchiveConfiguration.getSharedInstance());
	BucketFormatResolver bucketFormatResolver = new BucketFormatResolver(
		pathResolver, archiveFileSystem, bucketFormatChooser);

	Service mockedSplunkService = UtilsMockito
		.createSplunkServiceReturningThawPathForIndex(thawIndex,
			thawDirectoryLocation.getAbsolutePath());
	splunkSettings = new SplunkSettings(mockedSplunkService);
	ThawLocationProvider thawLocationProvider = new ThawLocationProvider(
		splunkSettings);
	ThawBucketTransferer thawBucketTransferer = new ThawBucketTransferer(
		thawLocationProvider, archiveFileSystem);
	bucketThawer = new BucketThawer(bucketsLister, bucketFilter,
		bucketFormatResolver, thawBucketTransferer);
    }

    private BucketFreezer getSuccessfulBucketFreezer() {
	File movedBucketsLocation = createDirectoryInParent(tempDirectory,
		ThawFunctionalTest.class.getName() + "-safeBuckets");
	BucketMover bucketMover = new BucketMover(
		movedBucketsLocation.getAbsolutePath());
	BucketLocker bucketLocker = new BucketLocker();
	ArchiveRestHandler archiveRestHandler = new ArchiveRestHandler(
		new DefaultHttpClient());

	return new BucketFreezer(bucketMover, bucketLocker, archiveRestHandler,
		mock(FailedBucketsArchiver.class));
    }

    @AfterMethod
    public void tearDown() throws IOException {
	FileUtils.deleteDirectory(tempDirectory);
	FileUtils.deleteDirectory(new File(BucketLock.DEFAULT_LOCKS_DIRECTORY));
	UtilsArchiverFunctional.getHadoopFileSystem().delete(tmpPath, true);
    }

    public void Thawer_givenExistingBucket_archiveItThenThawItBack()
	    throws FileNotFoundException, FileNotDirectoryException,
	    InterruptedException {
	Date earliest = new Date(1332295013);
	Date latest = new Date(earliest.getTime() + 26);

	try {
	    Bucket bucketToFreeze = UtilsBucket
		    .createBucketWithIndexAndTimeRange(thawIndex, earliest,
			    latest);
	    successfulBucketFreezer.freezeBucket(bucketToFreeze.getIndex(),
		    bucketToFreeze.getDirectory().getAbsolutePath());

	    assertFalse(bucketToFreeze.getDirectory().exists());

	    assertTrue(UtilsFile.isDirectoryEmpty(thawDirectoryLocation));
	    bucketThawer.thawBuckets(thawIndex, earliest, latest);
	    assertFalse(UtilsFile.isDirectoryEmpty(thawDirectoryLocation));

	    File[] listFiles = thawDirectoryLocation.listFiles();
	    assertEquals(1, listFiles.length);
	    assertEquals(bucketToFreeze.getName(), listFiles[0].getName());
	} finally {
	    UtilsArchiverFunctional.cleanArchivePathInHadoopFileSystem();
	}
    }

    public void Thawer_archivingBucketsInThreeDifferentTimeRanges_filterByOnlyOneOfTheTimeRanges()
	    throws IOException, InterruptedException {
	Date earliest = new Date(1332295013);
	Date latest = new Date(earliest.getTime() + 26);
	for (int i = 0; i < 3; i++) {
	    Date early = new Date(earliest.getTime() + i * 100);
	    Date later = new Date(early.getTime() + 30);
	    Bucket bucket = UtilsBucket.createBucketWithIndexAndTimeRange(
		    thawIndex, early, later);
	    successfulBucketFreezer.freezeBucket(bucket.getIndex(), bucket
		    .getDirectory().getAbsolutePath());
	    assertFalse(bucket.getDirectory().exists());
	}
	File thawLocation = splunkSettings.getThawLocation(thawIndex);
	int bucketsInThawLocation = thawLocation.listFiles().length;
	assertEquals(0, bucketsInThawLocation);

	bucketThawer.thawBuckets(thawIndex, earliest, latest);
	bucketsInThawLocation = thawLocation.listFiles().length;
	assertEquals(1, bucketsInThawLocation);
	FileUtils.forceDelete(thawLocation.listFiles()[0]);
	UtilsArchiverFunctional.cleanArchivePathInHadoopFileSystem();
    }

}
