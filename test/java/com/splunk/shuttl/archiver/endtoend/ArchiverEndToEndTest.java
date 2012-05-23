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
package com.splunk.shuttl.archiver.endtoend;

import static com.splunk.shuttl.testutil.UtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Service;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.ArchiveRestHandler;
import com.splunk.shuttl.archiver.archive.BucketFreezer;
import com.splunk.shuttl.archiver.archive.recovery.BucketLocker;
import com.splunk.shuttl.archiver.archive.recovery.BucketMover;
import com.splunk.shuttl.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shuttl.archiver.functional.UtilsArchiverFunctional;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.thaw.BucketThawer;
import com.splunk.shuttl.archiver.thaw.BucketThawerFactory;
import com.splunk.shuttl.archiver.thaw.SplunkSettings;
import com.splunk.shuttl.testutil.UtilsBucket;
import com.splunk.shuttl.testutil.UtilsFile;
import com.splunk.shuttl.testutil.UtilsMBean;

@Test(enabled = false, groups = { "end-to-end" })
public class ArchiverEndToEndTest {

    File tempDirectory;
    BucketFreezer successfulBucketFreezer;
    BucketThawer bucketThawer;
    SplunkSettings splunkSettings;
    String thawIndex;
    File thawDirectoryLocation;
    Path tmpPath;
    private ArchiveConfiguration archiveConfiguration;

    @Parameters(value = { "splunk.username", "splunk.password", "splunk.host",
	    "splunk.mgmtport", "hadoop.host", "hadoop.port" })
    public void setUp(String splunkUserName, String splunkPw,
	    String splunkHost, String splunkPort, String hadoopHost,
	    String hadoopPort) throws Exception {
	setUp(splunkUserName, splunkPw, splunkHost, splunkPort);
	archiveBucketAndThawItBack_assertThawedBucketHasSameNameAsFrozenBucket();
	tearDown(hadoopHost, hadoopPort);
    }

    private void setUp(String splunkUserName, String splunkPw,
	    String splunkHost, String splunkPort) throws IllegalIndexException {
	UtilsMBean.registerShuttlArchiverMBean();
	archiveConfiguration = ArchiveConfiguration.getSharedInstance();
	thawIndex = "shuttl";
	tempDirectory = createTempDirectory();
	successfulBucketFreezer = getSuccessfulBucketFreezer();

	// CONFIG
	Service service = new Service(splunkHost, Integer.parseInt(splunkPort));
	service.login(splunkUserName, splunkPw);
	assertTrue(service.getIndexes().containsKey(thawIndex));
	splunkSettings = BucketThawerFactory.getSplunkSettings(service);
	thawDirectoryLocation = splunkSettings.getThawLocation(thawIndex);
    }

    private BucketFreezer getSuccessfulBucketFreezer() {
	File movedBucketsLocation = createDirectoryInParent(tempDirectory,
		ArchiverEndToEndTest.class.getName() + "-safeBuckets");
	BucketMover bucketMover = new BucketMover(movedBucketsLocation);
	BucketLocker bucketLocker = new BucketLocker();
	ArchiveRestHandler archiveRestHandler = new ArchiveRestHandler(
		new DefaultHttpClient());

	return new BucketFreezer(bucketMover, bucketLocker, archiveRestHandler,
		mock(FailedBucketsArchiver.class));
    }

    private void archiveBucketAndThawItBack_assertThawedBucketHasSameNameAsFrozenBucket()
	    throws Exception {
	Date earliest = new Date(1332295013);
	Date latest = new Date(earliest.getTime() + 26);

	Bucket bucketToFreeze = UtilsBucket.createBucketWithIndexAndTimeRange(
		thawIndex, earliest, latest);
	successfulBucketFreezer.freezeBucket(bucketToFreeze.getIndex(),
		bucketToFreeze.getDirectory().getAbsolutePath());

	boolean bucketToFreezeExists = bucketToFreeze.getDirectory().exists();
	assertFalse(bucketToFreezeExists);

	assertTrue(isThawDirectoryEmpty());

	callRestToThawBuckets(thawIndex, earliest, latest);
	assertFalse(isThawDirectoryEmpty());

	File[] listFiles = thawDirectoryLocation.listFiles();
	assertEquals(1, listFiles.length);
	assertEquals(bucketToFreeze.getName(), listFiles[0].getName());
    }

    private boolean isThawDirectoryEmpty() {
	return UtilsFile.isDirectoryEmpty(thawDirectoryLocation);
    }

    private void callRestToThawBuckets(String index, Date earliest, Date latest)
	    throws Exception {
	String requestString = "http://localhost:9090/shuttl/rest/archiver/bucket/thaw?index="
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
    }

    private void tearDown(String hadoopHost, String hadoopPort) {
	FileUtils.deleteQuietly(tempDirectory);
	FileSystem hadoopFileSystem = UtilsArchiverFunctional
		.getHadoopFileSystem(hadoopHost, hadoopPort);
	for (File dir : thawDirectoryLocation.listFiles()) {
	    FileUtils.deleteQuietly(dir);
	}
	deleteArchivingTmpPath(hadoopFileSystem);
	deleteArchivingRoot(hadoopFileSystem);
    }

    private void deleteArchivingTmpPath(FileSystem hadoopFileSystem) {
	try {
	    URI configuredTmp = archiveConfiguration.getTmpDirectory();
	    hadoopFileSystem.delete(new Path(configuredTmp), true);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    private void deleteArchivingRoot(FileSystem hadoopFileSystem) {
	try {
	    URI configuredRoot = archiveConfiguration.getArchivingRoot();
	    hadoopFileSystem.delete(new Path(configuredRoot), true);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

}
