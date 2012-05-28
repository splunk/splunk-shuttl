// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.archiver.archive;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.net.URI;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveBucketTransferer;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketDeleter;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.importexport.BucketExporter;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketArchiverTest {

    private BucketArchiver bucketArchiver;
    private ArchiveConfiguration config;
    private BucketExporter exporter;

    private PathResolver pathResolver;
    private Bucket bucket;
    private ArchiveBucketTransferer archiveBucketTransferer;
    private BucketDeleter deletesBuckets;

    @BeforeMethod(groups = { "fast-unit" })
    public void setUp() {
	config = mock(ArchiveConfiguration.class);
	exporter = mock(BucketExporter.class);
	pathResolver = mock(PathResolver.class);
	archiveBucketTransferer = mock(ArchiveBucketTransferer.class);
	deletesBuckets = mock(BucketDeleter.class);
	bucketArchiver = new BucketArchiver(config, exporter, pathResolver,
		archiveBucketTransferer, deletesBuckets);

	bucket = TUtilsBucket.createTestBucket();
    }

    @Test(groups = { "fast-unit" })
    public void archiveBucket_shouldUseTheArchiveFormatForExportingTheBucket() {
	// Setup
	BucketFormat format = BucketFormat.SPLUNK_BUCKET;
	when(config.getArchiveFormat()).thenReturn(format);
	// Test
	bucketArchiver.archiveBucket(bucket);
	// Verification
	verify(exporter).exportBucketToFormat(bucket, format);
    }

    public void archiveBucket_shouldResolveArchivePathWithIndexBucketAndFormat() {
	BucketFormat format = BucketFormat.SPLUNK_BUCKET;
	when(config.getArchiveFormat()).thenReturn(format);
	when(exporter.exportBucketToFormat(eq(bucket), any(BucketFormat.class)))
		.thenReturn(bucket);
	bucketArchiver.archiveBucket(bucket);
	verify(pathResolver).resolveArchivePath(bucket);
    }

    public void archiveBucket_givenArchiveBucketTransferer_letTransfererTransferTheBucket() {
	URI path = URI.create("file:/some/path");
	Bucket exportedBucket = mock(Bucket.class);
	when(exporter.exportBucketToFormat(eq(bucket), any(BucketFormat.class)))
		.thenReturn(exportedBucket);
	when(pathResolver.resolveArchivePath(exportedBucket)).thenReturn(path);
	bucketArchiver.archiveBucket(bucket);
	verify(archiveBucketTransferer).transferBucketToArchive(exportedBucket,
		path);
    }

    public void archiveBucket_givenBucketAndBucketDeleter_deletesBucketWithBucketDeleter() {
	Bucket exportedBucket = TUtilsBucket.createTestBucket();
	when(exporter.exportBucketToFormat(eq(bucket), any(BucketFormat.class)))
		.thenReturn(exportedBucket);
	bucketArchiver.archiveBucket(bucket);
	verify(deletesBuckets).deleteBucket(bucket);
	verify(deletesBuckets).deleteBucket(exportedBucket);
    }

    public void archiveBucket_whenExceptionIsThrown_deleteExportedBucketButNotOriginalBucket() {
	Bucket exportedBucket = TUtilsBucket.createTestBucket();
	when(exporter.exportBucketToFormat(eq(bucket), any(BucketFormat.class)))
		.thenReturn(exportedBucket);
	when(pathResolver.resolveArchivePath(any(Bucket.class))).thenThrow(
		new RuntimeException());
	try {
	    bucketArchiver.archiveBucket(bucket);
	} catch (Throwable e) {
	    // Do nothing.
	}
	verify(deletesBuckets).deleteBucket(exportedBucket);
	verifyNoMoreInteractions(deletesBuckets);
    }

    public void archiveBucket_whenExportBucketIsSameAsOriginalBucket_deleteBucketTwice() {
	when(exporter.exportBucketToFormat(eq(bucket), any(BucketFormat.class)))
		.thenReturn(bucket);
	bucketArchiver.archiveBucket(bucket);
	verify(deletesBuckets, times(2)).deleteBucket(bucket);
    }
}
