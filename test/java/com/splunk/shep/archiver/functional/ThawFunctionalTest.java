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

import static com.splunk.shep.archiver.LocalFileSystemConstants.*;
import static com.splunk.shep.testutil.UtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.Service;
import com.splunk.shep.archiver.archive.ArchiveConfiguration;
import com.splunk.shep.archiver.archive.ArchiveRestHandler;
import com.splunk.shep.archiver.archive.BucketFreezer;
import com.splunk.shep.archiver.archive.recovery.BucketLocker;
import com.splunk.shep.archiver.archive.recovery.BucketMover;
import com.splunk.shep.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.IllegalIndexException;
import com.splunk.shep.archiver.thaw.BucketThawer;
import com.splunk.shep.archiver.thaw.BucketThawerFactory;
import com.splunk.shep.archiver.thaw.SplunkSettings;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsFile;
import com.splunk.shep.testutil.UtilsMBean;

@Test(enabled = false, groups = { "functional" })
public class ThawFunctionalTest {

    File tempDirectory;
    BucketFreezer successfulBucketFreezer;
    BucketThawer bucketThawer;
    SplunkSettings splunkSettings;
    String thawIndex;
    File thawDirectoryLocation;
    Path tmpPath;

    @BeforeMethod
    public void setUp() throws IllegalIndexException {
	UtilsMBean.registerShepArchiverMBean();
	tmpPath = new Path(ArchiveConfiguration.getSharedInstance()
		.getTmpDirectory());
	thawIndex = "shep";
	tempDirectory = createTempDirectory();
	successfulBucketFreezer = getSuccessfulBucketFreezer();

	// CONFIG
	Service service = new Service("localhost", 8089);
	service.login("admin", "changeme");
	service.getIndexes().containsKey(thawIndex);
	splunkSettings = BucketThawerFactory.getSplunkSettings(service);
	thawDirectoryLocation = splunkSettings.getThawLocation(thawIndex);

    }

    private BucketFreezer getSuccessfulBucketFreezer() {
	File movedBucketsLocation = createDirectoryInParent(tempDirectory,
		ThawFunctionalTest.class.getName() + "-safeBuckets");
	BucketMover bucketMover = new BucketMover(movedBucketsLocation);
	BucketLocker bucketLocker = new BucketLocker();
	ArchiveRestHandler archiveRestHandler = new ArchiveRestHandler(
		new DefaultHttpClient());

	return new BucketFreezer(bucketMover, bucketLocker, archiveRestHandler,
		mock(FailedBucketsArchiver.class));
    }

    @AfterMethod
    public void tearDown() throws IOException {
	FileUtils.deleteDirectory(tempDirectory);
	FileUtils.deleteDirectory(getArchiverDirectory());
	UtilsArchiverFunctional.getHadoopFileSystem().delete(tmpPath, true);
	for (File dir : thawDirectoryLocation.listFiles()) {
	    FileUtils.deleteDirectory(dir);
	}
    }

    public void Thawer_givenExistingBucket_archiveItThenThawItBack()
	    throws Exception {
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

	    callRestToThawBuckets(thawIndex, earliest, latest);
	    assertFalse(UtilsFile.isDirectoryEmpty(thawDirectoryLocation));

	    File[] listFiles = thawDirectoryLocation.listFiles();
	    assertEquals(1, listFiles.length);
	    assertEquals(bucketToFreeze.getName(), listFiles[0].getName());
	} finally {
	    UtilsArchiverFunctional.cleanArchivePathInHadoopFileSystem();
	}
    }

    private void callRestToThawBuckets(String index, Date earliest, Date latest)
	    throws Exception {
	String requestString = "http://localhost:9090/shep/rest/archiver/bucket/thaw?index="
		+ index
		+ "&from="
		+ earliest.getTime()
		+ "&to="
		+ latest.getTime();
	HttpGet request = new HttpGet(requestString);
	new DefaultHttpClient().execute(request);
    }

    public void Thawer_archivingBucketsInThreeDifferentTimeRanges_filterByOnlyOneOfTheTimeRanges()
	    throws Exception {
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
	int bucketsInThawLocation = thawDirectoryLocation.listFiles().length;
	assertEquals(0, bucketsInThawLocation);

	callRestToThawBuckets(thawIndex, earliest, latest);
	bucketsInThawLocation = thawDirectoryLocation.listFiles().length;
	assertEquals(1, bucketsInThawLocation);
	FileUtils.forceDelete(thawDirectoryLocation.listFiles()[0]);
	UtilsArchiverFunctional.cleanArchivePathInHadoopFileSystem();
    }

}
