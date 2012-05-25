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

import static org.mockito.Mockito.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveBucketTransferer;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.UtilsTestNG;

@Test(groups = { "fast-unit" })
public class ArchiveBucketTransfererTest {

    @Test(groups = { "fast-unit" })
    public void transferBucketToArchive_givenValidBucketAndUri_putBucketWithArchiveFileSystem() {
	ArchiveFileSystem archive = mock(ArchiveFileSystem.class);
	Bucket bucket = mock(Bucket.class);
	ArchiveBucketTransferer archiveBucketTransferer = new ArchiveBucketTransferer(
		archive);
	archiveBucketTransferer.transferBucketToArchive(bucket, getURI());
    }

    private URI getURI() {
	String uri = "file:/some/path";
	try {
	    return new URI(uri);
	} catch (URISyntaxException e) {
	    e.printStackTrace();
	    UtilsTestNG.failForException("Could not create uri: " + uri, e);
	    return null;
	}
    }
}
