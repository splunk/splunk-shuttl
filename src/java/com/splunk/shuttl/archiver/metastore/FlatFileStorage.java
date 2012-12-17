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
package com.splunk.shuttl.archiver.metastore;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.model.Bucket;

public class FlatFileStorage {

	private static final Logger logger = Logger.getLogger(FlatFileStorage.class);
	private LocalFileSystemPaths localFileSystemPaths;

	public FlatFileStorage(LocalFileSystemPaths localFileSystemPaths) {
		this.localFileSystemPaths = localFileSystemPaths;
	}

	/**
	 * @return a {@link File} that's unique for the bucket and its filename.
	 */
	public File getFlatFile(Bucket bucket, String fileName) {
		return new File(localFileSystemPaths.getMetadataDirectory(bucket), fileName);
	}

	/**
	 * Writes data to a file identified by a bucket and an extension.
	 */
	public void writeFlatFile(Bucket bucket, String fileName, String data) {
		File file = getFlatFile(bucket, fileName);
		writeFlatFile(file, data);
	}

	/**
	 * Writes data to an existing file, that can be read by the
	 * {@link FlatFileStorage} class.
	 */
	void writeFlatFile(File file, String data) {
		String content = data + "";
		try {
			file.getParentFile().mkdirs();
			file.createNewFile();
			FileUtils.write(file, content);
		} catch (IOException e) {
			logIOExceptionForWritingToFile(file, content, e);
			throw new RuntimeException(e);
		}
	}

	private void logIOExceptionForWritingToFile(File file, String content,
			IOException e) {
		logger.debug(did("Tried to write to file for BucketSizeFile.", e,
				"To write size to file", "file", file, "content", content));
	}

	/**
	 * @return first line of data read from the file.
	 */
	public String readFlatFile(File file) {
		try {
			return getFirstLineFromFile(file);
		} catch (Exception e) {
			throw new FlatFileReadException(e);
		}
	}

	private String getFirstLineFromFile(File file) throws IOException {
		return FileUtils.readLines(file).get(0);
	}

	public static class FlatFileReadException extends RuntimeException {

		private static final long serialVersionUID = 0L;

		public FlatFileReadException(Exception e) {
			super(e);
		}
	}
}
