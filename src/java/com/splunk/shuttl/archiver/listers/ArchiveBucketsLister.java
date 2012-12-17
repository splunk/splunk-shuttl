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
package com.splunk.shuttl.archiver.listers;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.RemoteBucket;

/**
 * Lists {@link Bucket}s in an {@link ArchiveFileSystem}.
 */
public class ArchiveBucketsLister {

	private final static Logger logger = Logger
			.getLogger(ArchiveBucketsLister.class);
	private final ArchivedIndexesLister indexesLister;
	private final PathResolver pathResolver;
	private final ArchiveFileSystem archiveFileSystem;

	/**
	 * 
	 * @param archiveFileSystem
	 *          to list {@link Bucket}s on.
	 * @param indexesLister
	 *          to list indexes where {@link Bucket}s can be listed.
	 * @param pathResolver
	 *          for resolving paths on the {@link ArchiveFileSystem}
	 */
	public ArchiveBucketsLister(ArchiveFileSystem archiveFileSystem,
			ArchivedIndexesLister indexesLister, PathResolver pathResolver) {
		this.archiveFileSystem = archiveFileSystem;
		this.indexesLister = indexesLister;
		this.pathResolver = pathResolver;
	}

	/**
	 * List buckets in an {@link ArchiveFileSystem}.<br/>
	 * Note: Buckets returned will have {@link BucketFormat} = null;
	 * 
	 * @return list of buckets with null {@link BucketFormat}.
	 */
	public List<Bucket> listBuckets() {
		List<Bucket> buckets = new ArrayList<Bucket>();
		for (String index : indexesLister.listIndexes())
			buckets.addAll(listBucketsInIndex(index));
		return buckets;
	}

	/**
	 * Lists {@link Bucket}s for an index that's been archived in an
	 * {@link ArchiveFileSystem}<br/>
	 * Note: Buckets returned will have {@link BucketFormat} = null;
	 * 
	 * @return {@link Bucket}s archived for an index.
	 */
	public List<Bucket> listBucketsInIndex(String index) {
		ArrayList<Bucket> buckets = new ArrayList<Bucket>();
		for (String pathToBucket : getPathToBucketsWithIndex(index))
			buckets.add(createBucketFromPathToBucket(pathToBucket));
		return buckets;
	}

	private List<String> getPathToBucketsWithIndex(String index) {
		String bucketsHome = pathResolver.getBucketsHome(index);
		List<String> pathsToBuckets = listBucketsHomeInArchive(bucketsHome);
		return pathsToBuckets;
	}

	private List<String> listBucketsHomeInArchive(String bucketsHome) {
		try {
			return archiveFileSystem.listPath(bucketsHome);
		} catch (IOException e) {
			logger.debug(did("Listed buckets at bucketsHome in archive file system",
					"Got IOException", "To list buckets that have been archived",
					"buckets_home", bucketsHome, "exception", e));
			throw new RuntimeException(e);
		}
	}

	private Bucket createBucketFromPathToBucket(String pathToBucket) {
		String bucketIndex = pathResolver.resolveIndexFromPathToBucket(pathToBucket);
		String bucketName = FilenameUtils.getBaseName(pathToBucket);
		return this.createBucketWithErrorHandling(pathToBucket, bucketIndex,
				bucketName);
	}

	private Bucket createBucketWithErrorHandling(String pathToBucket,
			String bucketIndex, String bucketName) {
		return new RemoteBucket(pathToBucket, bucketIndex, bucketName,
				(BucketFormat) null);
	}
}
