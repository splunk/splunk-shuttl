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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketExporter;
import com.splunk.shuttl.archiver.importexport.BucketFileCreator;
import com.splunk.shuttl.archiver.importexport.GetsBucketsExportFile;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.util.UtilsBucket;

public abstract class CsvCompressedExporter implements BucketExporter {

	protected final CsvExporter csvExporter;
	protected final BucketFileCreator bucketFileCreator;
	protected final GetsBucketsExportFile getsBucketsExportFile;

	public CsvCompressedExporter(CsvExporter csvExporter,
			BucketFileCreator bucketFileCreator,
			GetsBucketsExportFile getsBucketsExportFile) {
		this.csvExporter = csvExporter;
		this.bucketFileCreator = bucketFileCreator;
		this.getsBucketsExportFile = getsBucketsExportFile;
	}

	@Override
	public LocalBucket exportBucket(LocalBucket b) {
		LocalBucket exportedBucket = csvExporter.exportBucket(b);
		File csvFile = UtilsBucket.getCsvFile(exportedBucket);
		File compressedFile = getsBucketsExportFile.getExportFile(b,
				BucketFormat.extensionOfFormat(getFormat()));
		compress(csvFile, compressedFile);
		return bucketFileCreator.createBucketWithFile(compressedFile, b);
	}

	private void compress(File csvFile, File compressedOutFile) {
		InputStream in = null;
		OutputStream out = null;
		try {
			in = new BufferedInputStream(FileUtils.openInputStream(csvFile));
			out = getCodec().createOutputStream(
					new BufferedOutputStream(FileUtils.openOutputStream(
							compressedOutFile, false)));
			IOUtils.copyLarge(in, out);
			csvFile.delete();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(out);
		}
	}

	protected abstract BucketFormat getFormat();

	protected abstract CompressionCodec getCodec();
}
