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

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.model.BucketFactory;

public class CsvBzip2Importer extends CsvCompressedImporter {

	public CsvBzip2Importer(CsvImporter csvImporter, BucketFactory bucketFactory) {
		super(csvImporter, bucketFactory);
	}

	@Override
	protected BucketFormat getFormat() {
		return BucketFormat.CSV_BZIP2;
	}

	@Override
	protected CompressionCodec getCodec() {
		return new BZip2Codec();
	}

	public static BucketImporter create(CsvImporter csvImporter) {
		return new CsvBzip2Importer(csvImporter, new BucketFactory());
	}

}
