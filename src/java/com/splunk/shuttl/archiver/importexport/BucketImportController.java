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
package com.splunk.shuttl.archiver.importexport;

import java.util.HashMap;
import java.util.Map;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.csv.CsvBzip2Importer;
import com.splunk.shuttl.archiver.importexport.csv.CsvImporter;
import com.splunk.shuttl.archiver.importexport.csv.CsvSnappyImporter;
import com.splunk.shuttl.archiver.importexport.tgz.TgzImporter;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Restores a {@link Bucket} that's in any {@link BucketFormat} to
 * {@link BucketFormat#SPLUNK_BUCKET}.
 */
public class BucketImportController {

	private final Map<BucketFormat, BucketImporter> importers;

	/**
	 * @param importers
	 *          to import buckets from CSV to SPLUNK_BUCKET.
	 */
	public BucketImportController(Map<BucketFormat, BucketImporter> importers) {
		this.importers = importers;
	}

	/**
	 * @param bucket
	 *          to restore to {@link BucketFormat#SPLUNK_BUCKET}.
	 * @return bucket in {@link BucketFormat#SPLUNK_BUCKET}
	 */
	public LocalBucket restoreToSplunkBucketFormat(LocalBucket bucket) {
		BucketFormat format = bucket.getFormat();
		if (format.equals(BucketFormat.SPLUNK_BUCKET))
			return bucket;
		else if (importers.containsKey(format))
			return importers.get(format).importBucket(bucket);
		else
			throw new UnsupportedOperationException();
	}

	/**
	 * Convenience method for creating an instance.
	 */
	public static BucketImportController create() {
		Map<BucketFormat, BucketImporter> importers = new HashMap<BucketFormat, BucketImporter>();
		CsvImporter csvImporter = CsvImporter.create();
		importers.put(BucketFormat.CSV, csvImporter);
		importers.put(BucketFormat.SPLUNK_BUCKET_TGZ, TgzImporter.create());
		importers.put(BucketFormat.CSV_SNAPPY,
				CsvSnappyImporter.create(csvImporter));
		importers.put(BucketFormat.CSV_BZIP2, CsvBzip2Importer.create(csvImporter));

		return new BucketImportController(importers);
	}

}
