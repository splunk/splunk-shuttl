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

import java.io.File;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.util.UtilsFile;

/**
 * Creates a {@link Bucket} in {@link BucketFormat#CSV} from a .csv file and the
 * original {@link Bucket}.
 */
public class CsvBucketCreator {

	/**
	 * @return {@link Bucket} created from the specified .csv file and it's
	 *         original {@link Bucket}.
	 */
	public Bucket createBucketWithCsvFile(File file, Bucket bucket) {
		if (UtilsFile.isCsvFile(file))
			return doCreateBucketWithCsvFile(file, bucket);
		else
			throw new IllegalArgumentException("File was not csvFile: " + file);
	}

	private Bucket doCreateBucketWithCsvFile(File csvFile, Bucket bucket) {
		if (csvFile.exists())
			return createBucketWithExistingCsvFile(csvFile, bucket);
		else
			throw new CsvFileNotFoundException("Csv file does not exist: " + csvFile);
	}

	private Bucket createBucketWithExistingCsvFile(File csvFile, Bucket bucket) {
		File bucketDir = createBucketDirectory(csvFile);
		moveCsvFileToBucketDir(csvFile, bucketDir);
		return createBucketObject(csvFile, bucket, bucketDir);
	}

	private File createBucketDirectory(File csvFile) {
		String csvFileNameWithoutExtension = FilenameUtils.getBaseName(csvFile
				.getName());
		File bucketFile = new File(csvFile.getParentFile(),
				csvFileNameWithoutExtension);
		bucketFile.mkdirs();
		return bucketFile;
	}

	private void moveCsvFileToBucketDir(File csvFile, File bucketFile) {
		csvFile.renameTo(new File(bucketFile, csvFile.getName()));
	}

	private Bucket createBucketObject(File csvFile, Bucket bucket, File bucketDir) {
		try {
			String csvFileNameWithoutExtension = removeExtension(csvFile);
			return new Bucket(bucketDir.toURI(), bucket.getIndex(),
					csvFileNameWithoutExtension, BucketFormat.CSV, bucket.getSize());
		} catch (Exception e) {
			logBucketCreationException(bucket, bucketDir, e);
			throw new RuntimeException(e);
		}
	}

	private String removeExtension(File csvFile) {
		String csvFileNameWithoutExtension = FilenameUtils.removeExtension(csvFile
				.getName());
		return csvFileNameWithoutExtension;
	}

	private void logBucketCreationException(Bucket bucket, File bucketDir,
			Exception e) {
		Logger.getLogger(getClass()).error(
				did("Tried to create a bucket in CsvBucketCreator", e,
						"Bucket to be created", "bucket", bucket, "bucket_directory",
						bucketDir));
	}
}
