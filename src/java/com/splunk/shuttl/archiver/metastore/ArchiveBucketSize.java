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
package com.splunk.shuttl.archiver.metastore;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionExecuter;
import com.splunk.shuttl.archiver.metastore.MetadataStore.CouldNotReadMetadataException;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Gets a {@link Bucket}'s size. Both a remote bucket and a local bucket. <br/>
 * <br/>
 * This is needed because we want to know how big the {@link Bucket} will be on
 * the local file system and there's no guarantee that the size on the archive
 * file system is the same as on local disk. We therefore need to be able to put
 * and get the local file system size of the bucket, from the archive file
 * system.
 */
public class ArchiveBucketSize {

	/**
	 * For compatibility with older Shuttl versions.
	 */
	public static final String FILE_NAME = PathResolver.BUCKET_SIZE_FILE_NAME;

	private final MetadataStore metadataStore;

	public ArchiveBucketSize(MetadataStore metadataStore) {
		this.metadataStore = metadataStore;
	}

	/**
	 * @return size of an archived bucket on the local file system. Returns null
	 *         if the archiveSize is not persisted locally nor remotely.
	 */
	public Long readBucketSize(Bucket bucket) {
		try {
			return Long.parseLong(metadataStore.read(bucket,
					getSizeMetadataFileName()));
		} catch (CouldNotReadMetadataException e) {
			return null;
		}
	}

	/**
	 * @return file name of the metadata file with bucket size.
	 */
	public String getSizeMetadataFileName() {
		return FILE_NAME;
	}

	/**
	 * @param bucket
	 *          to persist bucket size for.
	 */
	public void persistBucketSize(Bucket bucket) {
		metadataStore.put(bucket, getSizeMetadataFileName(), "" + bucket.getSize());
	}

	/**
	 * Instance with path resolver and archive file system.
	 * 
	 * @param localFileSystemPaths
	 */
	public static ArchiveBucketSize create(PathResolver pathResolver,
			ArchiveFileSystem archiveFileSystem,
			LocalFileSystemPaths localFileSystemPaths) {
		return new ArchiveBucketSize(new MetadataStore(pathResolver,
				new FlatFileStorage(localFileSystemPaths), archiveFileSystem,
				new TransactionExecuter(), localFileSystemPaths));
	}
}
