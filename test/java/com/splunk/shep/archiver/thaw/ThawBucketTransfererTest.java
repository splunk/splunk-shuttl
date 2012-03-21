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

import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast" })
public class ThawBucketTransfererTest {

    ThawBucketTransferer bucketTransferer;
    Bucket bucket;
    ArchiveFileSystem archiveFileSystem;
    ThawLocationProvider thawLocationProvider;

    @BeforeMethod
    public void setUp() {
	bucket = UtilsBucket.createTestBucket();
	thawLocationProvider = mock(ThawLocationProvider.class);
	archiveFileSystem = mock(ArchiveFileSystem.class);
	bucketTransferer = new ThawBucketTransferer(thawLocationProvider,
		archiveFileSystem);
    }

    @Test(enabled = false, groups = { "fast" })
    public void transferBucketToThaw_givenBucket_transferBucketFromArchiveToPathWhereParentIsThawLocation()
	    throws IOException {
	File file = mock(File.class);
	when(thawLocationProvider.getLocationInThawForBucket(bucket))
		.thenReturn(file);
	bucketTransferer.transferBucketToThaw(bucket);

	verify(archiveFileSystem).getFile(file, bucket.getURI());
    }
}
