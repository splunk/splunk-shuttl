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
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.listers.ArchiveBucketsLister;
import com.splunk.shuttl.archiver.listers.ArchivedIndexesLister;
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
	URI bucketsHome = URI.create("valid:/uri/bucketsHome");
	when(pathResolver.getBucketsHome(anyString())).thenReturn(bucketsHome);
	archiveBucketsLister.listBucketsInIndex("index");
	verify(archiveFileSystem).listPath(bucketsHome);
    }

    public void listBucketsInIndex_listedBucketsHomeInArchive_resolveIndexFromUrisToBuckets()
	    throws IOException {
	String uriBase = "valid:/uri/bucketsHome/";
	URI bucketUri1 = URI.create(uriBase + "bucket1");
	URI bucketUri2 = URI.create(uriBase + "bucket2");
	List<URI> bucketsInBucketsHome = Arrays.asList(bucketUri1, bucketUri2);
	when(archiveFileSystem.listPath(any(URI.class))).thenReturn(
		bucketsInBucketsHome);
	archiveBucketsLister.listBucketsInIndex("index");
	for (URI uriToBucket : bucketsInBucketsHome)
	    verify(pathResolver).resolveIndexFromUriToBucket(uriToBucket);
    }

    public void listBucketsInIndex_givenUriToBucketsAndIndexToThoseBuckets_returnListOfBucketsNameAndIndexButNullFormat()
	    throws IOException {
	String uriBase = "valid:/uri/bucketsHome/";
	String index = "index";
	String bucketName1 = "bucket1";
	String bucketName2 = "bucket2";
	URI bucketUri1 = URI.create(uriBase + bucketName1);
	URI bucketUri2 = URI.create(uriBase + bucketName2);
	List<URI> bucketsInBucketsHome = Arrays.asList(bucketUri1, bucketUri2);
	when(archiveFileSystem.listPath(any(URI.class))).thenReturn(
		bucketsInBucketsHome);
	when(pathResolver.resolveIndexFromUriToBucket(any(URI.class)))
		.thenReturn(index);

	List<Bucket> buckets = archiveBucketsLister.listBucketsInIndex(index);
	assertEquals(2, buckets.size());
	Bucket bucket1 = new Bucket(bucketUri1, index, bucketName1, null, null);
	Bucket bucket2 = new Bucket(bucketUri2, index, bucketName2, null, null);
	for (Bucket bucket : buckets) {
	    assertTrue(TUtilsTestNG.isBucketEqualOnIndexFormatAndName(bucket1,
		    bucket)
		    || TUtilsTestNG.isBucketEqualOnIndexFormatAndName(bucket2,
			    bucket));
	}
    }
}
