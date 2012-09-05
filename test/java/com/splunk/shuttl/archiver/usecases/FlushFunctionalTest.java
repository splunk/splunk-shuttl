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
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.Date;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.flush.Flusher;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.thaw.SplunkSettings;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "functional" })
public class FlushFunctionalTest {

	private Flusher flusher;
	private String index;
	private File thawDir;
	private SplunkSettings splunkSettings;

	@BeforeMethod
	public void setUp() throws IllegalIndexException {
		splunkSettings = mock(SplunkSettings.class);
		thawDir = createDirectory();
		index = "index";
		when(splunkSettings.getThawLocation(index)).thenReturn(thawDir);
		flusher = new Flusher(splunkSettings);
	}

	public void _emptyThawDirectory_doesNothing() {
		assertTrue(isDirectoryEmpty(thawDir));
		flusher.flush(index, new Date(), new Date());
	}

	public void _thawDirectoryDoesNotExist_doesNothing()
			throws IllegalIndexException {
		File dir = createDirectory();
		assertTrue(dir.delete());
		when(splunkSettings.getThawLocation("foo")).thenReturn(dir);
		flusher.flush("foo", new Date(), new Date());
	}

	public void _givenThawedBucket_flushingTheTimeRangeOfThawedBucketDeletesBucket()
			throws IllegalIndexException {
		Bucket thawedBucket = TUtilsBucket.createBucketInDirectory(thawDir);
		assertTrue(thawedBucket.getDirectory().exists());
		flusher.flush(index, thawedBucket.getEarliest(), thawedBucket.getLatest());
		assertFalse(thawedBucket.getDirectory().exists());
	}

	public void _givenThawedBucket_flushOutsideTheTimeRangeOfBucketDoesNotDeleteBucket() {
		Date date = new Date(0);
		Date laterDate = new Date(9);
		Bucket bucket = TUtilsBucket.createBucketInDirectoryWithTimes(thawDir,
				date, date);
		assertTrue(bucket.getDirectory().exists());
		flusher.flush(index, laterDate, laterDate);
		assertTrue(bucket.getDirectory().exists());
	}

	public void _givenTwoThawedBuckets_withinTimeRangeDeletesBuckets() {
		Bucket b1 = TUtilsBucket.createBucketInDirectory(thawDir);
		Bucket b2 = TUtilsBucket.createBucketInDirectoryWithTimes(thawDir,
				b1.getEarliest(), b1.getLatest());
		flusher.flush(index, b1.getEarliest(), b1.getLatest());
		assertFalse(b1.getDirectory().exists());
		assertFalse(b2.getDirectory().exists());
	}

	public void _givenFileThatIsNotABucket_doesNotDeleteFile() {
		File file = createFileInParent(thawDir, "not.a.bucket");
		flusher.flush(index, new Date(), new Date());
		assertTrue(file.exists());
	}

	public void _givenDirectoryThatIsNotABucket_doesNotDeleteDir() {
		File dir = createDirectoryInParent(thawDir, "dir.is.not.a.bucket");
		flusher.flush(index, new Date(), new Date());
		assertTrue(dir.exists());
	}
}
