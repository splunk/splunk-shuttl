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
package com.splunk.shuttl.archiver.archive;

import java.util.List;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionExecuter;
import com.splunk.shuttl.archiver.importexport.BucketExportController;
import com.splunk.shuttl.archiver.importexport.csv.BucketToCsvFileExporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvBzip2Exporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvExporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvGzipExporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvSnappyExporter;
import com.splunk.shuttl.archiver.importexport.light.SplunkLightExporter;
import com.splunk.shuttl.archiver.importexport.tgz.CreatesBucketTgz;
import com.splunk.shuttl.archiver.importexport.tgz.TgzFormatExporter;
import com.splunk.shuttl.archiver.metastore.ArchiveBucketSize;

/**
 * Construction code for creating BucketArchivers that archives in different
 * FileSystems.
 */
public class BucketShuttlerFactory {

	/**
	 * @return {@link BucketArchiver} as configured in .conf files.
	 */
	public static BucketArchiver createConfiguredArchiver() {
		ArchiveConfiguration config = ArchiveConfiguration.getSharedInstance();
		return createWithConfig(config);
	}

	public static BucketCopier createCopierWithConfig(ArchiveConfiguration config) {
		BucketCopierDependencies deps = getDependencies(config,
				ArchiveFileSystemFactory.getWithConfiguration(config),
				LocalFileSystemPaths.create(config));
		return newCopierWithDependencies(deps);
	}

	private static BucketCopier newCopierWithDependencies(
			BucketCopierDependencies deps) {
		return new BucketCopier(deps.exporter, deps.transferer, deps.formats,
				deps.deleter);
	}

	/**
	 * Testability with specified configuration.
	 */
	public static BucketArchiver createWithConfig(ArchiveConfiguration config) {
		return createWithConfAndLocalPaths(config, LocalFileSystemPaths.create());
	}

	public static BucketArchiver createWithConfAndLocalPaths(
			ArchiveConfiguration config, LocalFileSystemPaths localFileSystemPaths) {
		ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
				.getWithConfiguration(config);
		return createWithConfFileSystemAndLocalPaths(config, archiveFileSystem,
				localFileSystemPaths);
	}

	/**
	 * @param config
	 *          for configuring the archiver
	 * @param archiveFileSystem
	 *          for storing the files
	 * @param csvDirectory
	 *          for storing csv exports.
	 * @return a {@link BucketArchiver} to archive with the specified
	 *         configuration.
	 */
	public static BucketArchiver createWithConfFileSystemAndLocalPaths(
			ArchiveConfiguration config, ArchiveFileSystem archiveFileSystem,
			LocalFileSystemPaths localFileSystemPaths) {
		BucketCopierDependencies deps = getDependencies(config, archiveFileSystem,
				localFileSystemPaths);

		return new BucketArchiver(newCopierWithDependencies(deps), deps.deleter);
	}

	private static BucketCopierDependencies getDependencies(
			ArchiveConfiguration config, ArchiveFileSystem archiveFileSystem,
			LocalFileSystemPaths localFileSystemPaths) {
		BucketToCsvFileExporter bucketToCsvFileExporter = BucketToCsvFileExporter
				.create(localFileSystemPaths);
		PathResolver pathResolver = new PathResolver(config);
		ArchiveBucketSize archiveBucketSize = ArchiveBucketSize.create(
				pathResolver, archiveFileSystem, localFileSystemPaths);

		TgzFormatExporter tgzFormatExporter = TgzFormatExporter
				.create(CreatesBucketTgz.create(localFileSystemPaths));

		CsvExporter csvExporter = CsvExporter.create(bucketToCsvFileExporter);
		BucketExportController bucketExportController = BucketExportController
				.create(csvExporter, tgzFormatExporter,
						CsvSnappyExporter.create(csvExporter, localFileSystemPaths),
						CsvBzip2Exporter.create(csvExporter, localFileSystemPaths),
						CsvGzipExporter.create(csvExporter, localFileSystemPaths),
						SplunkLightExporter.create(localFileSystemPaths,
								config.getFormatMetadata()));
		ArchiveBucketTransferer bucketTransferer = new ArchiveBucketTransferer(
				archiveFileSystem, pathResolver, archiveBucketSize,
				new TransactionExecuter());
		BucketDeleter bucketDeleter = BucketDeleter.create();
		List<BucketFormat> archiveFormats = config.getArchiveFormats();

		BucketCopierDependencies deps = new BucketCopierDependencies(
				bucketExportController, bucketTransferer, bucketDeleter, archiveFormats);
		return deps;
	}

	private static class BucketCopierDependencies {

		public BucketExportController exporter;
		public ArchiveBucketTransferer transferer;
		public BucketDeleter deleter;
		public List<BucketFormat> formats;

		public BucketCopierDependencies(
				BucketExportController bucketExportController,
				ArchiveBucketTransferer bucketTransferer, BucketDeleter bucketDeleter,
				List<BucketFormat> archiveFormats) {
			this.exporter = bucketExportController;
			this.transferer = bucketTransferer;
			this.deleter = bucketDeleter;
			this.formats = archiveFormats;
		}

	}
}
