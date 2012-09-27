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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.util.UtilsFile;

/**
 * Creates a {@link Bucket} in {@link BucketFormat#CSV} from a .csv file and the
 * original {@link Bucket}.
 */
public class BucketFileCreator {

	private BucketFormat format;
	private String extension;

	/**
	 * @param Format
	 *          which the bucket created should be.
	 */
	private BucketFileCreator(BucketFormat format, String extension) {
		this.format = format;
		this.extension = extension;
	}

	/**
	 * @return {@link Bucket} created from the specified .csv file and it's
	 *         original {@link Bucket}.
	 */
	public Bucket createBucketWithFile(File file, Bucket bucket) {
		if (file.getName().endsWith(extension))
			return doCreateBucketWithCsvFile(file, bucket);
		else
			throw new IllegalArgumentException("File: " + file
					+ ", did not have extension: " + extension);
	}

	private Bucket doCreateBucketWithCsvFile(File file, Bucket bucket) {
		if (file.exists())
			return createBucketWithExistingCsvFile(file, bucket);
		else
			throw new BucketFileNotFoundException("Bucket file does not exist: "
					+ file);
	}

	private Bucket createBucketWithExistingCsvFile(File file, Bucket bucket) {
		File bucketDir = createBucketDirectory(file);
		moveFileToBucketDir(file, bucketDir);
		return createBucketObject(file, bucket, bucketDir);
	}

	private File createBucketDirectory(File file) {
		File bucketFile = new File(file.getParentFile(), removeExtension(file));
		bucketFile.mkdirs();
		return bucketFile;
	}

	private void moveFileToBucketDir(File file, File bucketFile) {
		file.renameTo(new File(bucketFile, file.getName()));
	}

	private Bucket createBucketObject(File file, Bucket bucket, File bucketDir) {
		try {
			return new Bucket(bucketDir.toURI(), bucket.getIndex(),
					removeExtension(file), format, bucket.getSize());
		} catch (Exception e) {
			logBucketCreationException(bucket, bucketDir, e);
			throw new RuntimeException(e);
		}
	}

	private String removeExtension(File file) {
		return UtilsFile.getFileNameSansExt(file, extension);
	}

	private void logBucketCreationException(Bucket bucket, File bucketDir,
			Exception e) {
		Logger.getLogger(getClass()).error(
				did("Tried to create a bucket in CsvBucketCreator", e,
						"Bucket to be created", "bucket", bucket, "bucket_directory",
						bucketDir));
	}

	public static BucketFileCreator createForCsv() {
		return new BucketFileCreator(BucketFormat.CSV, "csv");
	}

	public static BucketFileCreator createForTgz() {
		return new BucketFileCreator(BucketFormat.SPLUNK_BUCKET_TGZ, "tgz");
	}

}
