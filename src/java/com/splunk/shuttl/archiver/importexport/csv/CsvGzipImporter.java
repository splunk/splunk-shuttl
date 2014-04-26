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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketImporter;
import com.splunk.shuttl.archiver.model.BucketFactory;

public class CsvGzipImporter extends CsvCompressedImporter {

	public CsvGzipImporter(CsvImporter csvImporter, BucketFactory bucketFactory) {
		super(csvImporter, bucketFactory);
	}

	@Override
	protected BucketFormat getFormat() {
		return BucketFormat.CSV_GZIP;
	}

	@Override
	protected CompressionCodec getCodec() {
		return new CompressionCodec() {

			@Override
			public String getDefaultExtension() {
				throw new UnsupportedOperationException();
			}

			@Override
			public Class<? extends Decompressor> getDecompressorType() {
				throw new UnsupportedOperationException();
			}

			@Override
			public Class<? extends Compressor> getCompressorType() {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompressionOutputStream createOutputStream(OutputStream arg0,
					Compressor arg1) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompressionOutputStream createOutputStream(OutputStream arg0)
					throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompressionInputStream createInputStream(InputStream arg0,
					Decompressor arg1) throws IOException {
				final GZIPInputStream gzipInputStream = new GZIPInputStream(arg0);
				return new CompressionInputStream(gzipInputStream) {

					@Override
					public int read() throws IOException {
						return gzipInputStream.read();
					}

					@Override
					public void resetState() throws IOException {
						throw new UnsupportedOperationException();
					}

					@Override
					public int read(byte[] arg0, int arg1, int arg2) throws IOException {
						return gzipInputStream.read(arg0, arg1, arg2);
					}
				};
			}

			@Override
			public CompressionInputStream createInputStream(InputStream arg0)
					throws IOException {
				return this.createInputStream(arg0, null);
			}

			@Override
			public Decompressor createDecompressor() {
				throw new UnsupportedOperationException();
			}

			@Override
			public Compressor createCompressor() {
				throw new UnsupportedOperationException();
			}
		};
	}

	public static BucketImporter create(CsvImporter csvImporter) {
		return new CsvGzipImporter(csvImporter, new BucketFactory());
	}

}
