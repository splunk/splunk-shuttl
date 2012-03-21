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
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
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

@Test(enabled = false, groups = { "super-slow" })
public class ThawFunctionalTest {

    File tempDirectory;
    BucketFreezer successfulBucketFreezer;
    BucketThawer bucketThawer;
    private ThawLocationProvider thawLocationProvider;

    @BeforeMethod
    public void setUp() {
	tempDirectory = createTempDirectory();
	successfulBucketFreezer = getSuccessfulBucketFreezer();

	PathResolver pathResolver = UtilsArchiverFunctional
		.getRealPathResolver();
	FileSystem hadoopFileSystem = UtilsArchiverFunctional
		.getHadoopFileSystem();
	HadoopFileSystemArchive archiveFileSystem = new HadoopFileSystemArchive(
		hadoopFileSystem);
	ArchivedIndexesLister indexesLister = new ArchivedIndexesLister(
		pathResolver, archiveFileSystem);
	ArchiveBucketsLister bucketsLister = new ArchiveBucketsLister(
		archiveFileSystem, indexesLister, pathResolver);
	BucketFilter bucketFilter = new BucketFilter();
	BucketFormatChooser bucketFormatChooser = new BucketFormatChooser(
		new ArchiveConfiguration());
	BucketFormatResolver bucketFormatResolver = new BucketFormatResolver(
		pathResolver, archiveFileSystem, bucketFormatChooser);

	SplunkSettings splunkSettings = new SplunkSettings(
		getLoggedInSplunkService());
	thawLocationProvider = new ThawLocationProvider(splunkSettings);
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

    private Service getLoggedInSplunkService() {
	// CONFIG
	String splunkHost = "localhost";
	int splunkMgmtPort = 8089;
	Service service = new Service(splunkHost, splunkMgmtPort);
	String user = "admin";
	String password = "changeme";
	service.login(user, password);
	return service;
    }

    @AfterMethod
    public void tearDown() throws IOException {
	FileUtils.deleteDirectory(tempDirectory);
	FileUtils.deleteDirectory(new File(BucketLock.DEFAULT_LOCKS_DIRECTORY));
    }

    public void Archiver_givenExistingBucket_archiveItThenThawItBack()
	    throws FileNotFoundException, FileNotDirectoryException,
	    InterruptedException {
	String shepIndex = "shep"; // CONFIG
	String bucketName = "db_1332295039_1332295013_0";
	File locationInThawForBucket = null;

	try {
	    Bucket bucketToFreeze = UtilsBucket
		    .createTestBucketWithIndexAndName(shepIndex, bucketName);
	    successfulBucketFreezer.freezeBucket(bucketToFreeze.getIndex(),
		    bucketToFreeze.getDirectory().getAbsolutePath());

	    assertFalse(bucketToFreeze.getDirectory().exists());

	    URI hadoopArchivedBucketURI = UtilsArchiverFunctional
		    .getHadoopArchivedBucketURI(bucketToFreeze);
	    Bucket bucketToThaw = new Bucket(hadoopArchivedBucketURI,
		    bucketToFreeze.getIndex(), bucketToFreeze.getName(),
		    bucketToFreeze.getFormat());
	    bucketThawer.thawBuckets(bucketToThaw.getIndex(),
		    bucketToThaw.getEarliest(), bucketToThaw.getLatest());

	    locationInThawForBucket = thawLocationProvider
		    .getLocationInThawForBucket(bucketToThaw);
	    assertTrue(locationInThawForBucket.exists());
	} finally {
	    FileUtils.deleteQuietly(locationInThawForBucket);
	    UtilsArchiverFunctional.cleanArchivePathInHadoopFileSystem();
	}
    }

}
