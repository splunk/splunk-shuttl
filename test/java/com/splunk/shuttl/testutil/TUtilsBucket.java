// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.testutil;

import static com.splunk.shuttl.testutil.TUtilsFile.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.testng.AssertJUnit;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketExporterIntegrationTest;
import com.splunk.shuttl.archiver.importexport.csv.CsvBucketCreator;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.archiver.util.UtilsBucket;

/**
 * Util for creating a physical and valid bucket on the file system.
 */
public class TUtilsBucket {

	/* package-private */static final URL REAL_BUCKET_URL = BucketExporterIntegrationTest.class
			.getResource("/splunk-buckets/SPLUNK_BUCKET/db_1336330530_1336330530_0");
	/* package-private */static final URL REAL_CSV_BUCKET_URL = BucketExporterIntegrationTest.class
			.getResource("/splunk-buckets/CSV/db_1336330530_1336330530_0");

	/**
	 * @return A bucket with random bucket and index names.
	 * @see #createTestBucketWithName(String, String)
	 */
	public static Bucket createBucket() {
		return createBucketWithIndexAndName(randomIndexName(), randomBucketName());

	}

	private static String randomIndexName() {
		return "index-" + System.currentTimeMillis();
	}

	private static String randomBucketName() {
		long latest = System.currentTimeMillis();
		long earliest = latest - RandomUtils.nextInt(10000);
		return String.format("db_%d_%d_%d", earliest, latest,
				RandomUtils.nextInt(1000));
	}

	/**
	 * @return A bucket with specified index and bucket names.
	 */
	public static Bucket createBucketWithIndexAndName(String index,
			String bucketName) {
		File bucketDir = createFileFormatedAsBucket(bucketName);
		return createBucketWithIndexInDirectory(index, bucketDir);
	}

	private static Bucket createBucketWithIndexInDirectory(String index,
			File bucketDir) {
		Bucket testBucket = null;
		try {
			testBucket = new Bucket(index, bucketDir);
		} catch (Exception e) {
			TUtilsTestNG.failForException("Couldn't create a test bucket", e);
			throw new RuntimeException(
					"There was a UtilsTestNG.failForException() method call above me that stoped me from happening. Where did it go?");
		}
		AssertJUnit.assertNotNull(testBucket);

		return testBucket;
	}

	/**
	 * @return A directory formated as a bucket.
	 */
	public static File createFileFormatedAsBucket(String bucketName) {
		File bucketDir = TUtilsFile.createDirectoryWithName(bucketName);
		return formatDirectoryToBeABucket(bucketDir);
	}

	private static File formatDirectoryToBeABucket(File bucketDir) {
		File rawdata = TUtilsFile.createDirectoryInParent(bucketDir, "rawdata");
		File slices = TUtilsFile.createFileInParent(rawdata, "slices.dat");
		TUtilsFile.populateFileWithRandomContent(slices);
		return bucketDir;
	}

	/**
	 * @param parent
	 *          directory to create bucket in.
	 * @return bucket created in parent.
	 */
	public static Bucket createBucketInDirectory(File parent) {
		return createBucketInDirectoryWithIndex(parent, randomIndexName());
	}

	/**
	 * @param parent
	 *          directory to create bucket in.
	 * @return bucket created in parent.
	 */
	public static Bucket createBucketInDirectoryWithIndex(File parent,
			String index) {
		File bucketDir = createFileFormatedAsBucketInDirectory(parent);
		return createBucketWithIndexInDirectory(index, bucketDir);
	}

	/**
	 * @param parent
	 *          to create the bucket in.
	 * @return File with the format of a bucket
	 */
	private static File createFileFormatedAsBucketInDirectory(File parent) {
		return createFileFormatedAsBucketInDirectoryWithName(parent,
				randomBucketName());
	}

	private static File createFileFormatedAsBucketInDirectoryWithName(
			File parent, String bucketName) {
		File child = TUtilsFile.createDirectoryInParent(parent, bucketName);
		return formatDirectoryToBeABucket(child);
	}

	/**
	 * Creates test bucket with earliest and latest times in its name.
	 */
	public static Bucket createBucketWithTimes(Date earliest, Date latest) {
		return createBucketWithIndexAndTimeRange(randomIndexName(), earliest,
				latest);
	}

	/**
	 * Creates test bucket with earliest and latest times in its name and index.
	 */
	public static Bucket createBucketWithIndexAndTimeRange(String index,
			Date earliest, Date latest) {
		String name = getNameWithEarliestAndLatestTime(earliest, latest);
		return createBucketWithIndexAndName(index, name);
	}

	private static String getNameWithEarliestAndLatestTime(Date earliest,
			Date latest) {
		return "db_" + latest.getTime() + "_" + earliest.getTime() + "_"
				+ randomIndexName();
	}

	/**
	 * Creates test bucket with earliest and latest time in a directory.
	 */
	public static Bucket createBucketInDirectoryWithTimes(File parent,
			Date earliest, Date latest) {
		String bucketName = getNameWithEarliestAndLatestTime(earliest, latest);
		File bucketDir = createFileFormatedAsBucketInDirectoryWithName(parent,
				bucketName);
		return createBucketWithIndexInDirectory(randomIndexName(), bucketDir);
	}

	/**
	 * Creates test bucket with specified name and random index.
	 */
	public static Bucket createBucketWithName(String name) {
		return createBucketWithIndexAndName(randomIndexName(), name);
	}

	/**
	 * @return bucket with real splunk bucket data in it.
	 */
	public static Bucket createRealBucket() {
		return copyBucketWithUrl(REAL_BUCKET_URL);
	}

	/**
	 * @return create csv bucket with real splunk data.
	 */
	public static Bucket createRealCsvBucket() {
		Bucket realCsvBucketCopy = copyBucketWithUrl(REAL_CSV_BUCKET_URL);
		File csvFile = UtilsBucket.getCsvFile(realCsvBucketCopy);
		CsvBucketCreator csvBucketCreator = new CsvBucketCreator();
		return csvBucketCreator.createBucketWithCsvFile(csvFile, realCsvBucketCopy);
	}

	private static Bucket copyBucketWithUrl(URL bucketUrl) {
		try {
			return createTempCopyOfBucketFromDirectory(new File(bucketUrl.toURI()));
		} catch (Exception e) {
			TUtilsTestNG.failForException("Could not create bucket", e);
			return null;
		}
	}

	private static Bucket createTempCopyOfBucketFromDirectory(File realBucketDir)
			throws FileNotFoundException, FileNotDirectoryException {
		File tempDirectory = createDirectory();
		File copyBucketDir = createDirectoryInParent(tempDirectory,
				realBucketDir.getName());
		copyDirectory(realBucketDir, copyBucketDir);
		return new Bucket("index", copyBucketDir);
	}

	private static void copyDirectory(File from, File to) {
		try {
			FileUtils.copyDirectory(from, to);
		} catch (IOException e) {
			TUtilsTestNG
					.failForException("Couldn't copy: " + from + ", to: " + to, e);
		}
	}

	/**
	 * @return {@link Bucket} that is "fake-remote". It's not really archived some
	 *         where, but the object represents a bucket that is remote.
	 */
	public static Bucket createRemoteBucket() {
		try {
			return new Bucket(URI.create("remote:/uri"), "itHasAnIndex",
					getNameWithEarliestAndLatestTime(new Date(), new Date()),
					BucketFormat.SPLUNK_BUCKET);
		} catch (Exception e) {
			TUtilsTestNG.failForException("Could not create remote bucket", e);
			return null;
		}
	}
}
