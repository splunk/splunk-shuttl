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
package com.splunk.shuttl.archiver.model;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.splunk.shuttl.archiver.archive.BucketFormat;

/**
 *
 */
public class LocalBucket extends Bucket {

	private final File directory;

	public LocalBucket(File directory, String index, BucketFormat format)
			throws FileNotFoundException, FileNotDirectoryException {
		this(directory, index, directory.getName(), format, null);
	}

	public LocalBucket(File directory, String index, BucketFormat format,
			Long size) throws FileNotFoundException, FileNotDirectoryException {
		this(directory, index, directory.getName(), format, size);
	}

	public LocalBucket(File directory, String index, String bucketName,
			BucketFormat format, Long size) throws FileNotFoundException,
			FileNotDirectoryException {
		super(verifyDirectory(directory).getAbsolutePath(), index, bucketName,
				format, setSizeOnNullSizedLocalBucket(directory, size));
		this.directory = directory;
	}

	private static Long setSizeOnNullSizedLocalBucket(File directory, Long size) {
		return size == null ? FileUtils.sizeOfDirectory(directory) : size;
	}

	private static File verifyDirectory(File directory)
			throws FileNotFoundException, FileNotDirectoryException {
		if (!directory.exists())
			throw new FileNotFoundException("Could not find directory: " + directory);
		else if (!directory.isDirectory())
			throw new FileNotDirectoryException("Directory " + directory
					+ " is not a directory");
		else
			return directory;
	}

	/**
	 * @return The directory that this bucket has its data in.
	 */
	public File getDirectory() {
		return directory;
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
	public String toString() {
		return "LocalBucket [getDirectory()=" + getDirectory() + ", getName()="
				+ getName() + ", getIndex()=" + getIndex() + ", getFormat()="
				+ getFormat() + ", getPath()=" + getPath() + ", getEarliest()="
				+ getEarliest() + ", getLatest()=" + getLatest() + ", getSize()="
				+ getSize() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((directory == null) ? 0 : directory.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		LocalBucket other = (LocalBucket) obj;
		if (directory == null) {
			if (other.directory != null)
				return false;
		} else if (!directory.equals(other.directory))
			return false;
		return true;
	}
}
