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

import static com.splunk.shuttl.ShuttlConstants.*;
import static com.splunk.shuttl.testutil.UtilsFile.*;
import static java.util.Arrays.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
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
import com.splunk.shuttl.testutil.UtilsTestNG;

@Test(groups = { "end-to-end" })
public class ArchiverEndToEndTest {

    File tempDirectory;
    BucketFreezer successfulBucketFreezer;
    BucketThawer bucketThawer;
    SplunkSettings splunkSettings;
    String thawIndex;
    File thawDirectoryLocation;
    Path tmpPath;
    private ArchiveConfiguration archiveConfiguration;
    private String shuttlHost;
    private int shuttlPort;

    @Parameters(value = { "splunk.username", "splunk.password", "splunk.host",
	    "splunk.mgmtport", "hadoop.host", "hadoop.port", "shuttl.host",
	    "shuttl.port" })
    public void archiveBucketAndThawItBack(String splunkUserName,
	    String splunkPw, String splunkHost, String splunkPort,
	    String hadoopHost, String hadoopPort, String shuttlHost,
	    String shuttlPort) throws Exception {
	setUp(splunkUserName, splunkPw, splunkHost, splunkPort, shuttlHost,
		shuttlPort);
	archiveBucketAndThawItBack_assertThawedBucketHasSameNameAsFrozenBucket();
	tearDown(hadoopHost, hadoopPort);
    }

    private void setUp(String splunkUserName, String splunkPw,
	    String splunkHost, String splunkPort, String shuttlHost,
	    String shuttlPort) throws IllegalIndexException {
	this.shuttlHost = shuttlHost;
	this.shuttlPort = Integer.parseInt(shuttlPort);
	UtilsMBean.registerShuttlArchiverMBean();
	archiveConfiguration = ArchiveConfiguration.getSharedInstance();
	thawIndex = "shuttl";
	tempDirectory = createTempDirectory();
	successfulBucketFreezer = getSuccessfulBucketFreezer();

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
	File thawedBucket = listFiles[0];
	assertEquals(bucketToFreeze.getName(), thawedBucket.getName());
    }

    private boolean isThawDirectoryEmpty() {
	return UtilsFile.isDirectoryEmpty(thawDirectoryLocation);
    }

    private void callRestToThawBuckets(String index, Date earliest, Date latest) {
	HttpPost thawPostRequest = getThawPostRequest(index, earliest, latest);
	executePostRequest(thawPostRequest);
    }

    private HttpPost getThawPostRequest(String index, Date earliest, Date latest) {
	URI thawEndpoint = getThawEndpoint();
	HttpPost httpPost = new HttpPost(thawEndpoint);
	List<BasicNameValuePair> postParams = asList(nameValue("index", index),
		nameValue("from", earliest.getTime()),
		nameValue("to", latest.getTime()));
	setParamsToPostRequest(httpPost, postParams);
	return httpPost;
    }

    private void setParamsToPostRequest(HttpPost httpPost,
	    List<BasicNameValuePair> postParams) {
	try {
	    httpPost.setEntity(new UrlEncodedFormEntity(postParams));
	} catch (UnsupportedEncodingException e) {
	    UtilsTestNG.failForException(
		    "Could not create url encoded form entity with params: "
			    + postParams, e);
	}
    }

    private void executePostRequest(HttpPost httpPost) {
	try {
	    new DefaultHttpClient().execute(httpPost);
	} catch (Exception e) {
	    UtilsTestNG.failForException("Could not execute post: " + httpPost,
		    e);
	}
    }

    private BasicNameValuePair nameValue(String name, Object index) {
	return new BasicNameValuePair(name, index.toString());
    }

    private URI getThawEndpoint() {
	return URI.create("http://" + shuttlHost + ":" + shuttlPort + "/"
		+ ENDPOINT_CONTEXT + ENDPOINT_ARCHIVER + ENDPOINT_BUCKET_THAW);
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
