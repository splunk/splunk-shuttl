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
package com.splunk.shuttl.archiver.bucketsize;

import java.io.File;
import java.net.URI;

import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;

/**
 * Resolves paths on the {@link ArchiveFileSystem} for the file containing
 * metadata about a bucket's size.
 */
public class BucketSizeFilePathResolver {

	/**
	 * @return path on archive file system for file with bucket's size.
	 */
	public URI resolveBucketSizeFilePath(File fileWithBucketSize,
			URI metadataFolderUri) {
		// TODO Auto-generated method stub
		// TODO push down the path resolver into this class, and maybe
		// BucketSizeFile ?
		return URI.create("fisk");
	}

}
