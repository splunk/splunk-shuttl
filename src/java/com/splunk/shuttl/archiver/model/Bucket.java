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

package com.splunk.shuttl.archiver.model;

import java.io.FileNotFoundException;
import java.util.Date;

import com.splunk.shuttl.archiver.archive.BucketFormat;

/**
 * Model representing a Splunk bucket
 */
public class Bucket {

	private final BucketFormat format;
	private final String indexName;
	private final BucketName bucketName;
	private final String path;
	protected final Long size; // size on file system in bytes

	/**
	 * Bucket created with a path which support remote and local buckets.
	 * 
	 * @param path
	 *          to bucket
	 * @param index
	 *          that the bucket belongs to.
	 * @param bucketName
	 *          that identifies the bucket
	 * @param format
	 *          of this bucket
	 * @throws FileNotFoundException
	 * @throws FileNotDirectoryException
	 */
	public Bucket(String path, String index, String bucketName,
			BucketFormat format) {
		this(path, index, bucketName, format, null);
	}

	public Bucket(String path, String index, String bucketName,
			BucketFormat format, Long size) {
		this.path = path;
		this.indexName = index;
		this.bucketName = new BucketName(bucketName);
		this.format = format;
		this.size = size;
	}

	/**
	 * @return The name of this bucket.
	 */
	public String getName() {
		return bucketName.getName();
	}

	/**
	 * @return The name of the index that this bucket belong to.
	 */
	public String getIndex() {
		return indexName;
	}

	/**
	 * @return {@link BucketFormat} of this bucket.
	 */
	public BucketFormat getFormat() {
		return format;
	}

	/**
	 * @return path of this bucket, which should be globaly unique.
	 */
	public String getPath() {
		return path;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Bucket other = (Bucket) obj;
		if (indexName == null) {
			if (other.indexName != null)
				return false;
		} else if (!indexName.equals(other.indexName))
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Bucket [format=" + format + ", indexName=" + indexName
				+ ", bucketName=" + bucketName + " bucketSize=" + size + ", path="
				+ path + "]";
	}

	/**
	 * @return {@link Date} with earliest time of indexed data in the bucket.
	 */
	public Date getEarliest() {
		return new Date(toMillis(bucketName.getEarliest()));
	}

	private long toMillis(long l) {
		return l * 1000;
	}

	/**
	 * @return {@link Date} with latest time of indexed data in the bucket.
	 */
	public Date getLatest() {
		return new Date(toMillis(bucketName.getLatest()));
	}

	public Long getSize() {
		return size;
	}

	/**
	 * @return the GUID for the bucket.
	 */
	public String getGuid() {
		return bucketName.getGuid();
	}

	/**
	 * @return true if the bucket is a replicated bucket.
	 */
	public boolean isReplicatedBucket() {
		return bucketName.getDB().equals("rb");
	}

	/**
	 * @return true if the bucket is the original bucket.
	 */
	public boolean isOriginalBucket() {
		try {
			return bucketName.getDB().equals("db");
		} catch (RuntimeException e) {
			return false;
		}
	}

}
