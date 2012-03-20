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

import static com.splunk.shep.archiver.ArchiverLogger.*;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.splunk.shep.archiver.archive.BucketFormat;
import com.splunk.shep.archiver.archive.PathResolver;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.util.UtilsURI;

/**
 * Resolves the format of a {@link Bucket} that is stored on a
 * {@link ArchiveFileSystem}.
 */
public class BucketFormatResolver {

    private final PathResolver pathResolver;
    private final ArchiveFileSystem archiveFileSystem;
    private final BucketFormatChooser bucketFormatChooser;

    /**
     * @param pathResolver
     *            to resolve formats home for buckets.
     * @param archiveFileSystem
     *            to list formats in.
     * @param bucketFormatChooser
     *            for deciding which of the format to thaw.
     */
    public BucketFormatResolver(PathResolver pathResolver,
	    ArchiveFileSystem archiveFileSystem,
	    BucketFormatChooser bucketFormatChooser) {
	this.pathResolver = pathResolver;
	this.archiveFileSystem = archiveFileSystem;
	this.bucketFormatChooser = bucketFormatChooser;
    }

    /**
     * @param buckets
     *            without {@link BucketFormat} set.
     * @return buckets with {@link BucketFormat} set.
     */
    public List<Bucket> resolveBucketsFormats(List<Bucket> buckets) {
	List<Bucket> bucketsWithFormat = new ArrayList<Bucket>();
	for (Bucket bucket : buckets) {
	    bucketsWithFormat.add(getBucketWithResolvedFormat(bucket));
	}
	return bucketsWithFormat;
    }

    private Bucket getBucketWithResolvedFormat(Bucket bucket) {
	URI formatsHomeForBucket = pathResolver
		.resolveFormatsHomeForBucket(bucket);
	List<URI> formatUris = getFormatUrisWithErrorHandling(
		formatsHomeForBucket, bucket);
	Set<BucketFormat> availableFormats = getBucketFormats(formatUris);
	BucketFormat chosenFormat = bucketFormatChooser
		.chooseBucketFormat(availableFormats);
	return createBucketWithErrorHandling(bucket, chosenFormat);
    }

    private Bucket createBucketWithErrorHandling(Bucket bucket,
	    BucketFormat chosenFormat) {
	try {
	    return new Bucket(bucket.getURI(), bucket.getIndex(),
		    bucket.getName(), chosenFormat);
	} catch (IOException e) {
	    did("Created bucket with format",
		    e,
		    "To create bucket from another bucket, only changing the format.",
		    "bucket", bucket, "bucket_format", chosenFormat,
		    "exception", e);
	    throw new RuntimeException(e);
	}
    }

    private Set<BucketFormat> getBucketFormats(List<URI> formatUris) {
	Set<BucketFormat> formats = new HashSet<BucketFormat>();
	for (URI uri : formatUris) {
	    String formatName = UtilsURI
		    .getFileNameWithTrimmedEndingFileSeparator(uri);
	    formats.add(BucketFormat.valueOf(formatName));
	}
	return formats;
    }

    private List<URI> getFormatUrisWithErrorHandling(URI formatsHomeForBucket,
	    Bucket bucket) {
	try {
	    return archiveFileSystem.listPath(formatsHomeForBucket);
	} catch (IOException e) {
	    warn("Listed formats home for a bucket", e,
		    "Will not list any formats for bucket", "formats_home",
		    formatsHomeForBucket, "bucket", bucket, "exception", e);
	    return new ArrayList<URI>(0);
	}
    }
}
