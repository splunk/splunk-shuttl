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
package com.splunk.shep.archiver.archive;

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.File;

import org.apache.log4j.Logger;

import com.splunk.shep.archiver.model.Bucket;

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
	if (isCsvFile(file)) {
	    return doCreateBucketWithCsvFile(file, bucket);
	} else {
	    throw new IllegalArgumentException("File was not csvFile: " + file);
	}
    }

    private boolean isCsvFile(File file) {
	return file.getName().endsWith(".csv");
    }

    private Bucket doCreateBucketWithCsvFile(File csvFile, Bucket bucket) {
	if (csvFile.exists()) {
	    return createBucketWithExistingCsvFile(csvFile, bucket);
	} else {
	    throw new CsvFileNotFoundException("Csv file does not exist: "
		    + csvFile);
	}
    }

    private Bucket createBucketWithExistingCsvFile(File csvFile, Bucket bucket) {
	File bucketDir = createBucketDirectory(csvFile);
	moveCsvFileToBucketDir(csvFile, bucketDir);
	return createBucketObject(csvFile, bucket, bucketDir);
    }

    private File createBucketDirectory(File csvFile) {
	File bucketFile = new File(csvFile.getParentFile(), "csvBucket");
	bucketFile.mkdirs();
	return bucketFile;
    }

    private void moveCsvFileToBucketDir(File csvFile, File bucketFile) {
	csvFile.renameTo(new File(bucketFile, csvFile.getName()));
    }

    private Bucket createBucketObject(File csvFile, Bucket bucket,
	    File bucketDir) {
	try {
	    return new Bucket(bucketDir.toURI(), bucket.getIndex(),
		    csvFile.getName(), BucketFormat.CSV);
	} catch (Exception e) {
	    logBucketCreationException(bucket, bucketDir, e);
	    throw new RuntimeException(e);
	}
    }

    private void logBucketCreationException(Bucket bucket, File bucketDir,
	    Exception e) {
	Logger.getLogger(getClass()).error(
		did("Tried to create a bucket in CsvBucketCreator", e,
			"Bucket to be created", "bucket", bucket,
			"bucket_directory", bucketDir));
    }
}
