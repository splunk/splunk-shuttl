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
package com.splunk.shuttl.archiver.flush;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static java.util.Arrays.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.Date;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.thaw.SplunkIndexesLayer;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsDate;

@Test(groups = { "fast-unit" })
public class FlusherTest {

	private Flusher flusher;
	private String index;
	private File thawDir;
	private SplunkIndexesLayer splunkIndexesLayer;

	@BeforeMethod
	public void setUp() throws IllegalIndexException {
		splunkIndexesLayer = mock(SplunkIndexesLayer.class);
		thawDir = createDirectory();
		index = "index";
		when(splunkIndexesLayer.getThawLocation(index)).thenReturn(thawDir);
		flusher = new Flusher(splunkIndexesLayer);
	}

	public void _emptyThawDirectory_flushesNothing() throws IllegalIndexException {
		assertTrue(isDirectoryEmpty(thawDir));
		flusher.flush(index, new Date(), new Date());
		assertTrue(flusher.getFlushedBuckets().isEmpty());
	}

	public void _thawDirectoryDoesNotExist_flushesNothing()
			throws IllegalIndexException {
		File dir = createDirectory();
		assertTrue(dir.delete());
		when(splunkIndexesLayer.getThawLocation(index)).thenReturn(dir);
		flusher.flush(index, new Date(), new Date());
		assertTrue(flusher.getFlushedBuckets().isEmpty());
	}

	public void _givenThawedBucket_flushingTheTimeRangeOfThawedBucketDeletesBucket()
			throws IllegalIndexException {
		LocalBucket thawedBucket = TUtilsBucket.createBucketInDirectoryWithIndex(
				thawDir, index);
		assertTrue(thawedBucket.getDirectory().exists());
		flusher.flush(index, thawedBucket.getEarliest(), thawedBucket.getLatest());
		assertFalse(thawedBucket.getDirectory().exists());
		assertEquals(asList(thawedBucket), flusher.getFlushedBuckets());
	}

	public void _givenThawedBucket_flushOutsideTheTimeRangeOfBucketDoesNotDeleteBucket()
			throws IllegalIndexException {
		Date date = TUtilsDate.getNowWithoutMillis();
		Date laterDate = TUtilsDate.getLaterDate(date);
		LocalBucket bucket = TUtilsBucket.createBucketInDirectoryWithTimes(thawDir,
				date, date);
		assertTrue(bucket.getDirectory().exists());
		flusher.flush(index, laterDate, laterDate);
		assertTrue(bucket.getDirectory().exists());
		assertTrue(flusher.getFlushedBuckets().isEmpty());
	}

	public void _givenTwoThawedBuckets_withinTimeRangeDeletesBuckets()
			throws IllegalIndexException {
		LocalBucket b1 = TUtilsBucket.createBucketInDirectoryWithIndex(thawDir,
				index);
		LocalBucket b2 = TUtilsBucket.createBucketInDirectoryWithTimesAndIndex(
				thawDir, b1.getEarliest(), b1.getLatest(), index);
		flusher.flush(index, b1.getEarliest(), b1.getLatest());
		assertFalse(b1.getDirectory().exists());
		assertFalse(b2.getDirectory().exists());
		assertEquals(2, flusher.getFlushedBuckets().size());
	}

	public void _givenFileThatIsNotABucket_doesNotDeleteFile()
			throws IllegalIndexException {
		File file = createFileInParent(thawDir, "not.a.bucket");
		flusher.flush(index, new Date(), new Date());
		assertTrue(file.exists());
		assertTrue(flusher.getFlushedBuckets().isEmpty());
	}

	public void _givenDirectoryThatIsNotABucket_doesNotDeleteDir()
			throws IllegalIndexException {
		File dir = createDirectoryInParent(thawDir, "dir.is.not.a.bucket");
		flusher.flush(index, new Date(), new Date());
		assertTrue(dir.exists());
		assertTrue(flusher.getFlushedBuckets().isEmpty());
	}
}
