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

import static com.splunk.shep.archiver.ArchiverLogger.*;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import com.splunk.shep.archiver.archive.PathResolver;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.model.Bucket;

/**
 * Lists {@link Bucket}s in an {@link ArchiveFileSystem}.
 */
public class ArchiveBucketsLister {

    private final ArchivedIndexesLister indexesLister;
    private final PathResolver pathResolver;
    private final ArchiveFileSystem archiveFileSystem;

    /**
     * TODO
     * 
     * @param archiveFileSystem
     */
    public ArchiveBucketsLister(ArchiveFileSystem archiveFileSystem,
	    ArchivedIndexesLister indexesLister, PathResolver pathResolver) {
	this.archiveFileSystem = archiveFileSystem;
	this.indexesLister = indexesLister;
	this.pathResolver = pathResolver;
    }

    /**
     * List buckets in an {@link ArchiveFileSystem}
     */
    public void listBuckets() {
	for (String index : indexesLister.listIndexes()) {
	    listBucketsInIndex(index);
	}
    }

    private void listBucketsInIndex(String index) {
	URI bucketsHome = pathResolver.getBucketsHome(index);
	listBucketsHomeInArchive(bucketsHome);
    }

    private List<URI> listBucketsHomeInArchive(URI bucketsHome) {
	try {
	    return archiveFileSystem.listPath(bucketsHome);
	} catch (IOException e) {
	    did("Listed buckets at bucketsHome in archive file system",
		    "Got IOException",
		    "To list buckets that has been archived", "buckets_home",
		    bucketsHome, "exception", e);
	    throw new RuntimeException(e);
	}
    }

}
