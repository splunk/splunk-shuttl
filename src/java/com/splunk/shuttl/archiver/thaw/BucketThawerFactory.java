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

import com.splunk.Service;
import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.bucketsize.ArchiveBucketSize;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.listers.ListsBucketsFiltered;
import com.splunk.shuttl.archiver.listers.ListsBucketsFilteredFactory;
import com.splunk.shuttl.archiver.model.BucketFactory;

/**
 * Factory for getting {@link BucketThawer}
 */
public class BucketThawerFactory {

	/**
	 * Default {@link BucketThawer} as configured with .conf files.
	 */
	public static BucketThawer createDefaultThawer() {
		Service splunkService = getLoggedInSplunkService();
		SplunkSettings splunkSettings = getSplunkSettings(splunkService);
		ArchiveConfiguration config = ArchiveConfiguration.getSharedInstance();
		return createWithConfigAndSplunkSettingsAndLocalFileSystemConstants(config,
				splunkSettings, LocalFileSystemPaths.create());
	}

	/**
	 * Factory method for testability.
	 */
	public static BucketThawer createWithConfigAndSplunkSettingsAndLocalFileSystemConstants(
			ArchiveConfiguration configuration, SplunkSettings splunkSettings,
			LocalFileSystemPaths localFileSystemConstants) {
		ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
				.getWithConfiguration(configuration);
		ThawLocationProvider thawLocationProvider = ThawLocationProvider
				.createWithSplunkSettingsAndThawTransferLocation(splunkSettings,
						localFileSystemConstants.getThawTransfersDirectory());

		ThawBucketTransferer thawBucketTransferer = getThawBucketTransferer(
				archiveFileSystem, thawLocationProvider);
		ListsBucketsFiltered listsBucketsFiltered = ListsBucketsFilteredFactory
				.create(configuration);
		GetsBucketsFromArchive getsBucketsFromArchive = new GetsBucketsFromArchive(
				thawBucketTransferer, BucketImporter.create(), new BucketSizeResolver(
						ArchiveBucketSize.create(configuration)));
		return new BucketThawer(listsBucketsFiltered, getsBucketsFromArchive,
				thawLocationProvider, new ThawBucketLocker(
						localFileSystemConstants.getThawLocksDirectory()));
	}

	private static ThawBucketTransferer getThawBucketTransferer(
			ArchiveFileSystem archiveFileSystem,
			ThawLocationProvider thawLocationProvider) {
		ThawBucketTransferer thawBucketTransferer = new ThawBucketTransferer(
				thawLocationProvider, archiveFileSystem, new BucketFactory());
		return thawBucketTransferer;
	}

	// TODO: Communicating with splunk through splunk home is not nice.
	// CONFIG
	private static Service getLoggedInSplunkService() {
		Service splunkService = new Service("localhost", 8089);
		splunkService.login("admin", "changeme");
		return splunkService;
	}

	/**
	 * @return
	 */
	public static SplunkSettings getSplunkSettings(Service splunkService) {
		return new SplunkSettings(splunkService);
	}
}
