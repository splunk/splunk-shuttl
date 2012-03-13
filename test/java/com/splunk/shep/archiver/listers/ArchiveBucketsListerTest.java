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

import java.io.IOException;
import java.net.URI;

import org.junit.Ignore;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.PathResolver;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;

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

    @Ignore
    public void listBuckets_listedBucketsHomeInArchive_listBucketsInThoseIndexes() {

    }
}
