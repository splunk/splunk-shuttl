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

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystemFactory;

/**
 * Factory for creating {@link BucketFormatResolver}s.
 */
public class BucketFormatResolverFactory {

	/**
	 * @return instance configured with specified config.
	 */
	public static BucketFormatResolver create(ArchiveConfiguration config) {
		ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
				.getWithConfiguration(config);
		PathResolver pathResolver = new PathResolver(config);
		BucketFormatChooser bucketFormatChooser = new BucketFormatChooser(config);
		return new BucketFormatResolver(pathResolver, archiveFileSystem,
				bucketFormatChooser);
	}

}
