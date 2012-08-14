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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketFormat;

/**
 * Model representing a Splunk bucket
 */
public class Bucket {

	private final static Logger logger = Logger.getLogger(Bucket.class);
	private final BucketFormat format;
	private final File directory;
	private final String indexName;
	private final BucketName bucketName;
	private final URI uri;
	private final Long size; // size on file system in bytes

	/**
	 * Bucket with an index and format<br/>
	 * 
	 * @param indexName
	 *          The name of the index that this bucket belongs to.
	 * @param directory
	 *          that is the bucket
	 * @throws FileNotFoundException
	 *           if the file doesn't exist
	 * @throws FileNotDirectoryException
	 *           if the file is not a directory
	 */
	public Bucket(String indexName, File directory, BucketFormat format)
			throws FileNotFoundException, FileNotDirectoryException {
		this(directory.toURI(), directory, indexName, directory.getName(), format,
				null);
	}

	/**
	 * Creates a bucket with index, directory and size.
	 * 
	 * @see Bucket#Bucket(String, File)
	 */
	public Bucket(String index, File directory, BucketFormat format, Long size)
			throws FileNotFoundException, FileNotDirectoryException {
		this(directory.toURI(), directory, index, directory.getName(), format, size);
	}

	/**
	 * Bucket created with an URI to support remote buckets.
	 * 
	 * @param uri
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
	public Bucket(URI uri, String index, String bucketName, BucketFormat format)
			throws FileNotFoundException, FileNotDirectoryException {
		this(uri, getFileFromUri(uri), index, bucketName, format, null);
	}

	/**
	 * Bucket created with an URI to support remote buckets.
	 * 
	 * @param uri
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
	public Bucket(URI uri, String index, String bucketName, BucketFormat format,
			Long size) throws FileNotFoundException, FileNotDirectoryException {
		this(uri, getFileFromUri(uri), index, bucketName, format, size);
	}

	private Bucket(URI uri, File directory, String index, String bucketName,
			BucketFormat format, Long size) throws FileNotFoundException,
			FileNotDirectoryException {
		this.uri = uri;
		this.directory = directory;
		this.indexName = index;
		this.bucketName = new BucketName(bucketName);
		this.format = format;
		verifyDirectoryExists(directory);
		this.size = size != null ? size : setSizeOnLocalBucket();
	}

	private Long setSizeOnLocalBucket() {
		return isUriSet() && !isRemote() ? FileUtils.sizeOfDirectory(directory)
				: null;
	}

	private static File getFileFromUri(URI uri) {
		if (uri != null && "file".equals(uri.getScheme()))
			return new File(uri);
		return null;
	}

	private void verifyDirectoryExists(File directory)
			throws FileNotFoundException, FileNotDirectoryException {
		if (!isUriSet() || isRemote())
			return; // Stop verifying.
		if (!directory.exists())
			throw new FileNotFoundException("Could not find directory: " + directory);
		else if (!directory.isDirectory())
			throw new FileNotDirectoryException("Directory " + directory
					+ " is not a directory");
	}

	private boolean isUriSet() {
		return uri != null;
	}

	/**
	 * @return The directory that this bucket has its data in.
	 */
	public File getDirectory() {
		if (directory == null) {
			logger
					.debug(did("Got directory from bucket",
							"Bucket was remote and can't instantiate a File.", "", "bucket",
							this));
			throw new RemoteBucketException();
		}
		return directory;
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
	 * @return {@link URI} of this bucket.
	 */
	public URI getURI() {
		return uri;
	}

	/**
	 * Deletes the bucket from the file system.
	 * 
	 * @throws IOException
	 *           if it's not possible to delete the directory
	 */
	public void deleteBucket() throws IOException {
		FileUtils.deleteDirectory(getDirectory());
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
		if (directory == null) {
			if (other.directory != null)
				return false;
		} else if (!directory.getAbsolutePath().equals(
				other.directory.getAbsolutePath()))
			return false;
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
		return "Bucket [format=" + format + ", directory=" + directory
				+ ", indexName=" + indexName + ", bucketName=" + bucketName
				+ " bucketSize=" + size + ", uri=" + uri + "]";
	}

	/**
	 * @return true if the bucket is not on the local file system.
	 */
	public boolean isRemote() {
		return !uri.getScheme().equals("file");
	}

	/**
	 * @return {@link Date} with earliest time of indexed data in the bucket.
	 */
	public Date getEarliest() {
		return new Date(bucketName.getEarliest());
	}

	/**
	 * @return {@link Date} with latest time of indexed data in the bucket.
	 */
	public Date getLatest() {
		return new Date(bucketName.getLatest());
	}

	public Long getSize() {
		return size;
	}

}
