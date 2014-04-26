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

import java.io.File;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketExporter;
import com.splunk.shuttl.archiver.importexport.BucketFileCreator;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Exports a bucket with SPLUNK_BUCKET format to a bucket with CSV format.
 */
public class CsvExporter implements BucketExporter {

	private BucketToCsvFileExporter bucketToCsvFileExporter;
	private BucketFileCreator bucketFileCreator;

	public CsvExporter(BucketToCsvFileExporter bucketToCsvFileExporter,
			BucketFileCreator bucketFileCreator) {
		this.bucketToCsvFileExporter = bucketToCsvFileExporter;
		this.bucketFileCreator = bucketFileCreator;
	}

	/**
	 * @return the specified bucket in {@link BucketFormat.CSV} format.
	 */
	@Override
	public LocalBucket exportBucket(LocalBucket bucket) {
		File csvFile = bucketToCsvFileExporter.exportBucketToCsv(bucket);
		return bucketFileCreator.createBucketWithFile(csvFile, bucket);
	}

	public static CsvExporter create(
			BucketToCsvFileExporter bucketToCsvFileExporter) {
		return new CsvExporter(bucketToCsvFileExporter, new BucketFileCreator(
				BucketFormat.CSV));
	}
}
