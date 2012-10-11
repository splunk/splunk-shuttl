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
package com.splunk.shuttl.archiver.listers;

import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "fast-unit" })
public class ArchiveBucketsListerTest {

	ArchiveBucketsLister archiveBucketsLister;

	ArchivedIndexesLister indexLister;
	PathResolver pathResolver;
	ArchiveFileSystem archiveFileSystem;

	@BeforeMethod
	public void setUp() {
		archiveFileSystem = mock(ArchiveFileSystem.class);
		indexLister = mock(ArchivedIndexesLister.class);
		pathResolver = mock(PathResolver.class);
		archiveBucketsLister = new ArchiveBucketsLister(archiveFileSystem,
				indexLister, pathResolver);
	}

	@Test(groups = { "fast-unit" })
	public void listBuckets_givenIndexesLister_useIndexesListerToGetIndexes() {
		when(indexLister.listIndexes()).thenReturn(asList("index"));
		List<Bucket> buckets = archiveBucketsLister.listBuckets();
		assertTrue(buckets.isEmpty());
	}

	public void listBucketsInIndex_givenIndexesFromIndexLister_getBucketsHomeFromPathResolver() {
		String index = "index";
		archiveBucketsLister.listBucketsInIndex(index);
		verify(pathResolver).getBucketsHome(index);
	}

	public void listBucketsInIndex_givenBucketsHome_listBucketsOnArchiveFilesSystem()
			throws IOException {
		String bucketsHome = "/path/to/bucketsHome";
		when(pathResolver.getBucketsHome(anyString())).thenReturn(bucketsHome);
		archiveBucketsLister.listBucketsInIndex("index");
		verify(archiveFileSystem).listPath(bucketsHome);
	}

	public void listBucketsInIndex_listedBucketsHomeInArchive_resolveIndexFromUrisToBuckets()
			throws IOException {
		String basePath = "/path/to/bucketsHome/";
		String bucketUri1 = basePath + "bucket1";
		String bucketUri2 = basePath + "bucket2";
		List<String> bucketsInBucketsHome = Arrays.asList(bucketUri1, bucketUri2);
		when(archiveFileSystem.listPath(anyString())).thenReturn(
				bucketsInBucketsHome);
		archiveBucketsLister.listBucketsInIndex("index");
		for (String uriToBucket : bucketsInBucketsHome)
			verify(pathResolver).resolveIndexFromUriToBucket(uriToBucket);
	}

	public void listBucketsInIndex_givenUriToBucketsAndIndexToThoseBuckets_returnListOfBucketsNameAndIndexButNullFormat()
			throws IOException {
		String basePath = "/path/to/bucketsHome/";
		String index = "index";
		String bucketName1 = "bucket1";
		String bucketName2 = "bucket2";
		String bucketPath1 = basePath + bucketName1;
		String bucketPath2 = basePath + bucketName2;
		List<String> bucketsInBucketsHome = Arrays.asList(bucketPath1, bucketPath2);
		when(archiveFileSystem.listPath(anyString())).thenReturn(
				bucketsInBucketsHome);
		when(pathResolver.resolveIndexFromUriToBucket(anyString())).thenReturn(
				index);

		List<Bucket> buckets = archiveBucketsLister.listBucketsInIndex(index);
		assertEquals(2, buckets.size());
		Bucket bucket1 = new Bucket(bucketPath1, index, bucketName1, null);
		Bucket bucket2 = new Bucket(bucketPath2, index, bucketName2, null);
		for (Bucket bucket : buckets)
			assertTrue(TUtilsTestNG
					.isBucketEqualOnIndexFormatAndName(bucket1, bucket)
					|| TUtilsTestNG.isBucketEqualOnIndexFormatAndName(bucket2, bucket));
	}
}
