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
package com.splunk.shep.archiver.thaw;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.BucketFormat;
import com.splunk.shep.archiver.archive.PathResolver;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast" })
public class BucketFormatResolverTest {

    BucketFormatResolver bucketFormatResolver;
    PathResolver pathResolver;
    ArchiveFileSystem archiveFileSystem;
    BucketFormatChooser bucketFormatChooser;

    List<Bucket> mockedBucketsList;

    @BeforeMethod
    public void setUp() {
	mockedBucketsList = Arrays.asList(createBucketWithMockedURI());

	pathResolver = mock(PathResolver.class);
	archiveFileSystem = mock(ArchiveFileSystem.class);
	bucketFormatChooser = mock(BucketFormatChooser.class);
	bucketFormatResolver = new BucketFormatResolver(pathResolver,
		archiveFileSystem, bucketFormatChooser);
    }

    private Bucket createBucketWithMockedURI() {
	Bucket bucketWithMockedURI = mock(Bucket.class);
	when(bucketWithMockedURI.getURI()).thenReturn(URI.create("valid:/uri"));
	return bucketWithMockedURI;
    }

    @Test(groups = { "fast" })
    public void resolveBucketsFormats_givenValidBucket_askPathResolverForFormatsHomes() {
	Bucket bucket = createBucketWithMockedURI();
	bucketFormatResolver.resolveBucketsFormats(Arrays.asList(bucket));
	verify(pathResolver).resolveFormatsHomeForBucket(bucket);
    }

    public void resolveBucketsFormats_givenFormatsHomeForBucket_listFormatsHomeInArchiveFileSystem()
	    throws IOException {
	URI formatsHome = URI.create("valid:/uri");
	when(pathResolver.resolveFormatsHomeForBucket(any(Bucket.class)))
		.thenReturn(formatsHome);
	bucketFormatResolver.resolveBucketsFormats(mockedBucketsList);
	verify(archiveFileSystem).listPath(formatsHome);
    }

    public void resolveBucketsFormats_givenFormatUris_directoryNameIsFormat()
	    throws IOException {
	BucketFormat format = BucketFormat.SPLUNK_BUCKET;
	URI formatURI = URI.create("valid:/uri/" + format);
	when(archiveFileSystem.listPath(any(URI.class))).thenReturn(
		Arrays.asList(formatURI));
	bucketFormatResolver.resolveBucketsFormats(mockedBucketsList);
	verify(bucketFormatChooser).chooseBucketFormat(
		new HashSet<BucketFormat>(Arrays.asList(format)));
    }

    @SuppressWarnings("unchecked")
    public void resolveBucketsFormats_givenBucketToResolveForAndChosenFormat_listOfBucketWithFormatIndexAndName() {
	Bucket bucket = UtilsBucket.createTestBucket();
	BucketFormat format = BucketFormat.SPLUNK_BUCKET;
	when(bucketFormatChooser.chooseBucketFormat(anySet())).thenReturn(
		format);
	List<Bucket> bucketsWithFormat = bucketFormatResolver
		.resolveBucketsFormats(Arrays.asList(bucket));

	// Verify
	assertEquals(1, bucketsWithFormat.size());
	assertEquals(format, bucketsWithFormat.get(0).getFormat());
	assertEquals(bucket.getIndex(), bucketsWithFormat.get(0).getIndex());
	assertEquals(bucket.getName(), bucketsWithFormat.get(0).getName());
    }
}
