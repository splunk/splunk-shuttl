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
package com.splunk.shuttl.archiver.thaw;

import static java.util.Arrays.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.ArrayList;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.copy.IndexStoragePaths;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class LocalBucketStorageTest {

	private LocalBucketStorage localBucketStorage;
	private IndexStoragePaths indexStoragePaths;
	private LocalBucket bucket;

	@BeforeMethod
	public void setUp() {
		indexStoragePaths = mock(IndexStoragePaths.class);
		localBucketStorage = new LocalBucketStorage(indexStoragePaths);

		bucket = TUtilsBucket.createBucket();
	}

	public void hasBucket_indexHasNoStoragePaths_false() {
		when(indexStoragePaths.getDbPathsForIndex(bucket.getIndex())).thenReturn(
				new ArrayList<File>());
		assertFalse(localBucketStorage.hasBucket(bucket));
	}

	public void hasBucket_bucketWithTheSameNameExistsLocally_true() {
		LocalBucket bucketWithSameName = TUtilsBucket.createBucketWithName(bucket
				.getName());
		when(indexStoragePaths.getDbPathsForIndex(bucket.getIndex())).thenReturn(
				asList(bucketWithSameName.getDirectory().getParentFile()));

		assertTrue(localBucketStorage.hasBucket(bucket));
	}
}
