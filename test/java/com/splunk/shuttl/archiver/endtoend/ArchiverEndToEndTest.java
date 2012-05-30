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
import static com.splunk.shuttl.testutil.TUtilsFile.*;
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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
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
import com.splunk.shuttl.archiver.functional.UtilsFunctional;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.thaw.BucketThawer;
import com.splunk.shuttl.archiver.thaw.BucketThawerFactory;
import com.splunk.shuttl.archiver.thaw.SplunkSettings;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFile;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsTestNG;

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
			String splunkPw, String splunkHost, String splunkPort, String hadoopHost,
			String hadoopPort, String shuttlHost, String shuttlPort) throws Exception {
		setUp(splunkUserName, splunkPw, splunkHost, splunkPort, shuttlHost,
				shuttlPort);
		try {
			archiveBucketAndThawItBack_assertThawedBucketHasSameNameAsFrozenBucket();
		} finally {
			tearDown(hadoopHost, hadoopPort);
		}
	}

	private void setUp(String splunkUserName, String splunkPw, String splunkHost,
			String splunkPort, String shuttlHost, String shuttlPort)
			throws IllegalIndexException {
		this.shuttlHost = shuttlHost;
		this.shuttlPort = Integer.parseInt(shuttlPort);
		TUtilsMBean.registerShuttlArchiverMBean();
		archiveConfiguration = ArchiveConfiguration.getSharedInstance();
		thawIndex = "shuttl";
		tempDirectory = createDirectory();
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

		Bucket bucketToFreeze = TUtilsBucket.createBucketWithIndexAndTimeRange(
				thawIndex, earliest, latest);
		successfulBucketFreezer.freezeBucket(bucketToFreeze.getIndex(),
				bucketToFreeze.getDirectory().getAbsolutePath());

		verifyFreezeByListingBucketInArchive(bucketToFreeze, thawIndex, earliest,
				latest);

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

	private void verifyFreezeByListingBucketInArchive(Bucket bucket,
			String index, Date earliest, Date latest) {
		HttpGet listRequest = getListGetRequest(index, earliest, latest);
		HttpResponse response = executeUriRequest(listRequest);
		assertEquals(200, response.getStatusLine().getStatusCode());
		List<String> lines = getLinesFromResponse(response);
		assertEquals(1, lines.size());
		assertTrue(lines.get(0).contains(
				"\"bucketName\":\"" + bucket.getName() + "\""));
	}

	private HttpGet getListGetRequest(String index, Date earliest, Date latest) {
		return new HttpGet(getArchiverEndpoint(ENDPOINT_LIST_BUCKETS)
				+ createQuery(index, earliest, latest));
	}

	private String createQuery(String index, Date earliest, Date latest) {
		return "?index=" + index + "&from=" + earliest.getTime() + "&to="
				+ latest.getTime();
	}

	private List<String> getLinesFromResponse(HttpResponse response) {
		try {
			return IOUtils.readLines(response.getEntity().getContent());
		} catch (IOException e) {
			TUtilsTestNG.failForException("Could not read lines for http response.",
					e);
			return null;
		}
	}

	private boolean isThawDirectoryEmpty() {
		return TUtilsFile.isDirectoryEmpty(thawDirectoryLocation);
	}

	private void callRestToThawBuckets(String index, Date earliest, Date latest) {
		HttpPost thawPostRequest = getThawPostRequest(index, earliest, latest);
		executeUriRequest(thawPostRequest);
	}

	private HttpPost getThawPostRequest(String index, Date earliest, Date latest) {
		URI thawEndpoint = getArchiverEndpoint(ENDPOINT_BUCKET_THAW);
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
			TUtilsTestNG
					.failForException(
							"Could not create url encoded form entity with params: "
									+ postParams, e);
		}
	}

	private HttpResponse executeUriRequest(HttpUriRequest request) {
		try {
			return new DefaultHttpClient().execute(request);
		} catch (Exception e) {
			TUtilsTestNG.failForException(
					"Could not execute uri request: " + request, e);
			return null;
		}
	}

	private BasicNameValuePair nameValue(String name, Object index) {
		return new BasicNameValuePair(name, index.toString());
	}

	private URI getArchiverEndpoint(String endpoint) {
		return URI.create("http://" + shuttlHost + ":" + shuttlPort + "/"
				+ ENDPOINT_CONTEXT + ENDPOINT_ARCHIVER + endpoint);
	}

	private void tearDown(String hadoopHost, String hadoopPort) {
		FileUtils.deleteQuietly(tempDirectory);
		FileSystem hadoopFileSystem = UtilsFunctional.getHadoopFileSystem(
				hadoopHost, hadoopPort);
		for (File dir : thawDirectoryLocation.listFiles())
			FileUtils.deleteQuietly(dir);
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