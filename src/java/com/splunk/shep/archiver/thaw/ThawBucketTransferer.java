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

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.log4j.Logger;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.model.Bucket;

/**
 * Transfers bucket to thaw.
 */
public class ThawBucketTransferer {

    private final static Logger logger = Logger
	    .getLogger(ThawBucketTransferer.class);
    private final ThawLocationProvider thawLocationProvider;
    private final ArchiveFileSystem archiveFileSystem;

    public ThawBucketTransferer(ThawLocationProvider thawLocationProvider,
	    ArchiveFileSystem archiveFileSystem) {
	this.thawLocationProvider = thawLocationProvider;
	this.archiveFileSystem = archiveFileSystem;
    }

    /**
     * Transfers an archived bucket in the thaw directory of the bucket's index.
     */
    public void transferBucketToThaw(Bucket bucket) {
	File bucketsThawLocation = thawLocationProvider
		.getLocationInThawForBucket(bucket);
	thawBucketFromArchiveWithErrorHandling(bucketsThawLocation,
		bucket.getURI());
    }

    private void thawBucketFromArchiveWithErrorHandling(
	    File bucketsThawLocation, URI bucketArchiveUri) {
	try {
	    archiveFileSystem.getFile(bucketsThawLocation, bucketArchiveUri);
	} catch (IOException e) {
	    logger.debug(did("Tried to thaw bucket", e,
		    "Place the bucket in thaw", "bucket_thaw_location",
		    bucketsThawLocation, "archived_bucket_uri",
		    bucketArchiveUri, "exception", e));
	}
    }
}