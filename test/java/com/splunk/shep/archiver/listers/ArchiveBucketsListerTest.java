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
package com.splunk.shep.archiver.listers;

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

import com.splunk.shep.archiver.archive.PathResolver;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsTestNG;

@Test(groups = { "fast" })
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

    public void listBuckets_givenIndexesLister_useIndexesListerToGetIndexes() {
	archiveBucketsLister.listBuckets();
	verify(indexLister).listIndexes();
    }

    public void listBuckets_givenIndexesFromIndexLister_getBucketsHomeFromPathResolver() {
	String index = "index";
	when(indexLister.listIndexes()).thenReturn(asList(index));
	archiveBucketsLister.listBuckets();
	verify(pathResolver).getBucketsHome(index);
    }

    public void listBuckets_givenBucketsHome_listBucketsOnArchiveFilesSystem()
	    throws IOException {
	String index = "index";
	when(indexLister.listIndexes()).thenReturn(asList(index));
	URI bucketsHome = URI.create("valid:/uri/bucketsHome");
	when(pathResolver.getBucketsHome(anyString())).thenReturn(bucketsHome);
	archiveBucketsLister.listBuckets();
	verify(archiveFileSystem).listPath(bucketsHome);
    }

    public void listBuckets_listedBucketsHomeInArchive_resolveIndexFromUrisToBuckets()
	    throws IOException {
	String uriBase = "valid:/uri/bucketsHome/";
	URI bucketUri1 = URI.create(uriBase + "bucket1");
	URI bucketUri2 = URI.create(uriBase + "bucket2");
	List<URI> bucketsInBucketsHome = Arrays.asList(bucketUri1, bucketUri2);
	when(indexLister.listIndexes()).thenReturn(
		asList("indexToAnyIndexForListingBucketsHomeInIndex"));
	when(archiveFileSystem.listPath(any(URI.class))).thenReturn(
		bucketsInBucketsHome);
	archiveBucketsLister.listBuckets();
	for (URI uriToBucket : bucketsInBucketsHome)
	    verify(pathResolver).resolveIndexFromUriToBucket(uriToBucket);
    }

    public void listBuckets_givenUriToBucketsAndIndexToThoseBuckets_returnListOfBucketsNameAndIndexButNullFormat()
	    throws IOException {
	String uriBase = "valid:/uri/bucketsHome/";
	String bucketName1 = "bucket1";
	URI bucketUri1 = URI.create(uriBase + bucketName1);
	String bucketName2 = "bucket2";
	URI bucketUri2 = URI.create(uriBase + bucketName2);
	List<URI> bucketsInBucketsHome = Arrays.asList(bucketUri1, bucketUri2);
	String index = "index";
	when(indexLister.listIndexes()).thenReturn(asList(index));
	when(archiveFileSystem.listPath(any(URI.class))).thenReturn(
		bucketsInBucketsHome);
	when(pathResolver.resolveIndexFromUriToBucket(any(URI.class)))
		.thenReturn(index);

	List<Bucket> buckets = archiveBucketsLister.listBuckets();
	assertEquals(2, buckets.size());
	Bucket bucket1 = new Bucket(bucketUri1, index, bucketName1, null);
	Bucket bucket2 = new Bucket(bucketUri2, index, bucketName2, null);
	for (Bucket bucket : buckets) {
	    assertTrue(UtilsTestNG.isBucketEqualOnIndexFormatAndName(bucket1,
		    bucket)
		    || UtilsTestNG.isBucketEqualOnIndexFormatAndName(bucket2,
			    bucket));
	}
    }
}
