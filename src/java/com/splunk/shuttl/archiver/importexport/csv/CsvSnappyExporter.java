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

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketFileCreator;
import com.splunk.shuttl.archiver.importexport.GetsBucketsExportFile;

public class CsvSnappyExporter extends CsvCompressedExporter {

	public CsvSnappyExporter(CsvExporter csvExporter,
			BucketFileCreator bucketFileCreator,
			GetsBucketsExportFile getsBucketsExportFile) {
		super(csvExporter, bucketFileCreator, getsBucketsExportFile);
	}

	@Override
	protected BucketFormat getFormat() {
		return BucketFormat.CSV_SNAPPY;
	}

	@Override
	protected CompressionCodec getCodec() {
		return new SnappyCodec();
	}

	public static CsvSnappyExporter create(CsvExporter csvExporter,
			LocalFileSystemPaths localFileSystemPaths) {
		return new CsvSnappyExporter(csvExporter, new BucketFileCreator(
				BucketFormat.CSV_SNAPPY), new GetsBucketsExportFile(
				localFileSystemPaths));
	}
}
