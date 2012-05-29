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
package com.splunk.shuttl.archiver.importexport.csv;

import static com.splunk.shuttl.archiver.LogFormatter.*;
import static java.util.Arrays.*;

import java.io.File;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.ShellExecutor;
import com.splunk.shuttl.archiver.importexport.csv.splunk.SplunkImportTool;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.util.UtilsBucket;

/**
 * Imports Csv buckets and creates a Splunk Bucket.
 */
public class CsvImporter {

	private final SplunkImportTool splunkImportTool;
	private final ShellExecutor shellExecutor;
	private final BucketFactory bucketFactory;

	/**
	 * Uses the {@link ShellExecutor} to invoke the {@link SplunkImportTool} and
	 * import a bucket.
	 */
	public CsvImporter(SplunkImportTool splunkImportTool,
			ShellExecutor shellExecutor, BucketFactory bucketFactory) {
		this.splunkImportTool = splunkImportTool;
		this.shellExecutor = shellExecutor;
		this.bucketFactory = bucketFactory;
	}

	/**
	 * Takes a bucket in {@link BucketFormat.CSV} and converts it to a bucket with
	 * {@link BucketFormat.SPLUNK_BUCKET}.
	 * 
	 * @throws {@link IllegalArgumentException} if bucket is not in
	 *         {@link BucketFormat#CSV} format.
	 */
	public Bucket importBucketFromCsv(Bucket bucket) {
		if (!isBucketInCsvFormat(bucket))
			throw new IllegalArgumentException("Bucket not in csv format");
		else
			return getImportedBucketCsvBucket(bucket);
	}

	private Bucket getImportedBucketCsvBucket(Bucket bucket) {
		String[] importCommand = createCommandForImportingBucket(bucket);
		int exit = executeImportCommand(importCommand);
		Bucket newBucket = createNewBucket(bucket);
		deleteCsvFileOnSuccessfulImport(bucket, exit);
		return newBucket;
	}

	private int executeImportCommand(String[] fullCommand) {
		int exit = shellExecutor.executeCommand(splunkImportTool.getEnvironment(),
				asList(fullCommand));
		return exit;
	}

	private String[] createCommandForImportingBucket(Bucket bucket) {
		String executableImportTool = splunkImportTool.getExecutableCommand();
		File bucketDirectory = bucket.getDirectory();
		String[] fullCommand = new String[] { executableImportTool,
				bucketDirectory.getAbsolutePath(),
				UtilsBucket.getCsvFile(bucket).getAbsolutePath() };
		return fullCommand;
	}

	private void deleteCsvFileOnSuccessfulImport(Bucket bucket, int exit) {
		if (isSuccessfulImport(exit))
			deleteCsvFileInBucket(bucket);
	}

	private boolean isSuccessfulImport(int exit) {
		return exit == 0;
	}

	private void deleteCsvFileInBucket(Bucket bucket) {
		boolean deleted = UtilsBucket.getCsvFile(bucket).delete();
		if (!deleted)
			logUnsuccessfulDelete(bucket);
	}

	private void logUnsuccessfulDelete(Bucket bucket) {
		Logger logger = Logger.getLogger(getClass());
		logger.warn(warn("Tried deleting csv file after successful import.",
				"Delete was not successful.", "Ignoring", "bucket", bucket));
	}

	private boolean isBucketInCsvFormat(Bucket csvBucket) {
		return csvBucket.getFormat().equals(BucketFormat.CSV);
	}

	private Bucket createNewBucket(Bucket bucket) {
		return bucketFactory.createWithIndexAndDirectory(bucket.getIndex(),
				bucket.getDirectory());
	}

	/**
	 * @return {@link CsvImporter} with default construction logic.
	 */
	public static CsvImporter create() {
		return new CsvImporter(new SplunkImportTool(), new ShellExecutor(
				Runtime.getRuntime()), new BucketFactory());
	}

}
