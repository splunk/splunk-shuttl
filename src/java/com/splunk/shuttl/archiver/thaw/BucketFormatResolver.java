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
package com.splunk.shuttl.archiver.thaw;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.util.UtilsURI;

/**
 * Uses {@link ArchiveFileSystem} and {@link PathResolver} to list available
 * bucket formats. Then it uses {@link BucketFormatChooser} to choose a format
 * of the available ones.
 */
public class BucketFormatResolver {

	private final static Logger logger = Logger
			.getLogger(BucketFormatResolver.class);
	private final PathResolver pathResolver;
	private final ArchiveFileSystem archiveFileSystem;
	private final BucketFormatChooser bucketFormatChooser;

	/**
	 * @param pathResolver
	 *          to resolve formats home for buckets.
	 * @param archiveFileSystem
	 *          to list formats in.
	 * @param bucketFormatChooser
	 *          for deciding which of the format to thaw.
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
	 *          without {@link BucketFormat} set.
	 * @return buckets with {@link BucketFormat} set.
	 */
	public List<Bucket> resolveBucketsFormats(List<Bucket> buckets) {
		List<Bucket> bucketsWithFormat = new ArrayList<Bucket>();
		for (Bucket bucket : buckets)
			bucketsWithFormat.add(getBucketWithResolvedFormat(bucket));
		return bucketsWithFormat;
	}

	private Bucket getBucketWithResolvedFormat(Bucket bucket) {
		List<BucketFormat> availableFormats = getAvailableFormatsForBucket(bucket);
		BucketFormat chosenFormat = bucketFormatChooser
				.chooseBucketFormat(availableFormats);
		URI uriToBucketWithChosenBucket = pathResolver.resolveArchivedBucketURI(
				bucket.getIndex(), bucket.getName(), chosenFormat);
		return createBucketWithErrorHandling(bucket, chosenFormat,
				uriToBucketWithChosenBucket);
	}

	private List<BucketFormat> getAvailableFormatsForBucket(Bucket bucket) {
		URI formatsHomeForBucket = pathResolver.getFormatsHome(bucket.getIndex(),
				bucket.getName());
		List<URI> archivedFormats = listArchivedFormatsWithErrorHandling(
				formatsHomeForBucket, bucket);
		return getBucketFormats(archivedFormats);
	}

	private List<URI> listArchivedFormatsWithErrorHandling(
			URI formatsHomeForBucket, Bucket bucket) {
		try {
			return archiveFileSystem.listPath(formatsHomeForBucket);
		} catch (IOException e) {
			logger.warn(warn("Listed formats home for a bucket", e,
					"Will not list any formats for bucket", "formats_home",
					formatsHomeForBucket, "bucket", bucket, "exception", e));
			return Collections.emptyList();
		}
	}

	private List<BucketFormat> getBucketFormats(List<URI> formatUris) {
		List<BucketFormat> formats = new ArrayList<BucketFormat>();
		for (URI uri : formatUris) {
			String formatName = UtilsURI
					.getFileNameWithTrimmedEndingFileSeparator(uri);
			formats.add(BucketFormat.valueOf(formatName));
		}
		return formats;
	}

	private Bucket createBucketWithErrorHandling(Bucket bucket,
			BucketFormat chosenFormat, URI uriToBucketWithChosenBucket) {
		try {
			return new Bucket(uriToBucketWithChosenBucket, bucket.getIndex(),
					bucket.getName(), chosenFormat);
		} catch (IOException e) {
			logger.debug(did("Created bucket with format", e,
					"To create bucket from another bucket, only changing the format.",
					"bucket", bucket, "bucket_format", chosenFormat, "exception", e));
			throw new RuntimeException(e);
		}
	}
}
