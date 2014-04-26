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
import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.util.UtilsBucket;
import com.splunk.shuttl.archiver.util.UtilsFile;

public abstract class CsvCompressedImporter implements BucketImporter {

	private final CsvImporter csvImporter;
	private final BucketFactory bucketFactory;

	public CsvCompressedImporter(CsvImporter csvImporter,
			BucketFactory bucketFactory) {
		this.csvImporter = csvImporter;
		this.bucketFactory = bucketFactory;
	}

	@Override
	public LocalBucket importBucket(LocalBucket b) {
		decompress(b);
		LocalBucket csvBucket = bucketFactory.createWithIndexDirectoryAndFormat(
				b.getIndex(), b.getDirectory(), BucketFormat.CSV);
		return csvImporter.importBucket(csvBucket);
	}

	private void decompress(LocalBucket b) {
		File file = UtilsBucket.getFileFromBucket(b, getFormat());
		File csvFile = getCsvFile(b, file);
		InputStream in = null;
		OutputStream out = null;
		try {
			in = getCodec().createInputStream(
					new BufferedInputStream(FileUtils.openInputStream(file)));
			out = new BufferedOutputStream(FileUtils.openOutputStream(csvFile, false));
			IOUtils.copyLarge(in, out);
			file.delete();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(out);
		}
	}

	private File getCsvFile(LocalBucket b, File file) {
		String csvFilename = UtilsFile.getFileNameSansExt(file,
				BucketFormat.extensionOfFormat(getFormat()))
				+ BucketFormat.extensionOfFormat(BucketFormat.CSV);
		return new File(b.getDirectory(), csvFilename);
	}

	protected abstract BucketFormat getFormat();

	protected abstract CompressionCodec getCodec();

}
