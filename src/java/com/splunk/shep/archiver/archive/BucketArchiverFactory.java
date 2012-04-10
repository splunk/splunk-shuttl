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
package com.splunk.shep.archiver.archive;

import java.net.URI;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystemFactory;
import com.splunk.shep.archiver.fileSystem.WritableFileSystem;

/**
 * Construction code for creating BucketArchivers that archives in different
 * FileSystems.
 */
public class BucketArchiverFactory {

    public static BucketArchiver createConfiguredArchiver() {
	ArchiveConfiguration config = ArchiveConfiguration.getSharedInstance();
	ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
		.getConfiguredArchiveFileSystem();

	return new BucketArchiver(config, new BucketExporter(),
		getPathResolver(config), new ArchiveBucketTransferer(
			archiveFileSystem));
    }

    // TODO Remove the writable file system and expect that the configured
    // archiving root is writable.
    private static PathResolver getPathResolver(
	    final ArchiveConfiguration config) {
	PathResolver pathResolver = new PathResolver(config,
		new WritableFileSystem() {

		    @Override
		    public URI getWritableUri() {
			return config.getArchivingRoot();
		    }
		});
	return pathResolver;
    }

}
