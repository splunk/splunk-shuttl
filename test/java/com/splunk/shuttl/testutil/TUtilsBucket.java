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
import java.net.URL;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.RandomUtils;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketExportControllerIntegrationTest;
import com.splunk.shuttl.archiver.importexport.BucketFileCreator;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.model.RemoteBucket;
import com.splunk.shuttl.archiver.util.UtilsBucket;

/**
 * Util for creating a physical and valid bucket on the file system.
 */
public class TUtilsBucket {

	/* package-private */static final URL REAL_BUCKET_URL = BucketExportControllerIntegrationTest.class
			.getResource("/splunk-buckets/SPLUNK_BUCKET/db_1336330530_1336330530_0");
	/* package-private */static final URL REAL_CSV_BUCKET_URL = BucketExportControllerIntegrationTest.class
			.getResource("/splunk-buckets/CSV/db_1336330530_1336330530_0");
	/* package-private */static final URL REAL_SPLUNK_BUCKET_TGZ_URL = BucketExportControllerIntegrationTest.class
			.getResource("/splunk-buckets/SPLUNK_BUCKET_TGZ/db_1336330530_1336330530_0");

	/**
	 * @return A bucket with random bucket and index names.
	 * @see #createTestBucketWithName(String, String)
	 */
	public static LocalBucket createBucket() {
		return createBucketWithIndexAndName(randomIndexName(), randomBucketName());

	}

	private static String randomIndexName() {
		return "index-" + System.currentTimeMillis();
	}

	private static String randomBucketName() {
		long latest = System.currentTimeMillis() / 1000;
		long earliest = latest - (RandomUtils.nextInt(10) + 1);
		return String.format("db_%d_%d_%d", latest, earliest,
				RandomUtils.nextInt(1000));
	}

	/**
	 * @return A bucket with specified index and bucket names.
	 */
	public static LocalBucket createBucketWithIndexAndName(String index,
			String bucketName) {
		File bucketDir = createFileFormatedAsBucket(bucketName);
		return createBucketWithIndexInDirectory(index, bucketDir);
	}

	private static LocalBucket createBucketWithIndexInDirectory(String index,
			File bucketDir) {
		return createBucketWithIndexInDirectoryAndFormat(index, bucketDir,
				BucketFormat.SPLUNK_BUCKET);
	}

	private static LocalBucket createBucketWithIndexInDirectoryAndFormat(
			String index, File bucketDir, BucketFormat format) {
		try {
			return new LocalBucket(bucketDir, index, format);
		} catch (Exception e) {
			TUtilsTestNG.failForException("Couldn't create a test bucket", e);
			throw new RuntimeException(
					"There was a UtilsTestNG.failForException() method call above me that stoped me from happening. Where did it go?");
		}
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
	public static LocalBucket createBucketInDirectory(File parent) {
		return createBucketInDirectoryWithIndex(parent, randomIndexName());
	}

	/**
	 * @param parent
	 *          directory to create bucket in.
	 * @return bucket created in parent.
	 */
	public static LocalBucket createBucketInDirectoryWithIndex(File parent,
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
	public static LocalBucket createBucketWithTimes(Date earliest, Date latest) {
		return createBucketWithIndexAndTimeRange(randomIndexName(), earliest,
				latest);
	}

	/**
	 * Creates test bucket with earliest and latest times in its name and index.
	 */
	public static LocalBucket createBucketWithIndexAndTimeRange(String index,
			Date earliest, Date latest) {
		String name = getNameWithEarliestAndLatestTime(earliest, latest);
		return createBucketWithIndexAndName(index, name);
	}

	private static String getNameWithEarliestAndLatestTime(Date earliest,
			Date latest) {
		return "db_" + toSec(latest.getTime()) + "_" + toSec(earliest.getTime())
				+ "_" + randomIndexName();
	}

	private static long toSec(long time) {
		return time / 1000;
	}

	/**
	 * Creates test bucket with earliest and latest time in a directory.
	 */
	public static LocalBucket createBucketInDirectoryWithTimes(File parent,
			Date earliest, Date latest) {
		return createBucketInDirectoryWithTimesAndIndex(parent, earliest, latest,
				randomIndexName());
	}

	public static LocalBucket createBucketInDirectoryWithTimesAndIndex(
			File parent, Date earliest, Date latest, String index) {
		String bucketName = getNameWithEarliestAndLatestTime(earliest, latest);
		File bucketDir = createFileFormatedAsBucketInDirectoryWithName(parent,
				bucketName);
		return createBucketWithIndexInDirectory(index, bucketDir);
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
	public static LocalBucket createRealBucket() {
		return copyBucketWithUrl(REAL_BUCKET_URL);
	}

	/**
	 * @return create csv bucket with real splunk data.
	 */
	public static LocalBucket createRealCsvBucket() {
		LocalBucket realCsvBucketCopy = copyBucketWithUrl(REAL_CSV_BUCKET_URL);
		File csvFile = UtilsBucket.getCsvFile(realCsvBucketCopy);
		BucketFileCreator bucketFileCreator = BucketFileCreator.createForCsv();
		return bucketFileCreator.createBucketWithFile(csvFile, realCsvBucketCopy);
	}

	public static LocalBucket createRealSplunkBucketTgz() {
		LocalBucket realTgzBucketCopy = copyBucketWithUrl(REAL_SPLUNK_BUCKET_TGZ_URL);
		File tgzFile = UtilsBucket.getTgzFile(realTgzBucketCopy);
		return BucketFileCreator.createForTgz().createBucketWithFile(tgzFile,
				realTgzBucketCopy);
	}

	private static LocalBucket copyBucketWithUrl(URL bucketUrl) {
		try {
			return createTempCopyOfBucketFromDirectory(new File(bucketUrl.toURI()));
		} catch (Exception e) {
			TUtilsTestNG.failForException("Could not create bucket", e);
			return null;
		}
	}

	private static LocalBucket createTempCopyOfBucketFromDirectory(
			File realBucketDir) throws FileNotFoundException,
			FileNotDirectoryException {
		File copyBucketDir = createDirectoryInParent(createDirectory(),
				realBucketDir.getName());
		copyDirectory(realBucketDir, copyBucketDir);
		return new LocalBucket(copyBucketDir, "index", BucketFormat.SPLUNK_BUCKET);
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
	public static RemoteBucket createRemoteBucket() {
		try {
			return new RemoteBucket("/path", "itHasAnIndex",
					getNameWithEarliestAndLatestTime(new Date(), new Date()),
					BucketFormat.SPLUNK_BUCKET);
		} catch (Exception e) {
			TUtilsTestNG.failForException("Could not create remote bucket", e);
			return null;
		}
	}

	/**
	 * @return {@link Bucket} with a tgz format
	 */
	public static LocalBucket createTgzBucket() {
		File bucketDir = createDirectory();
		try {
			new File(bucketDir, randomBucketName() + ".tgz").createNewFile();
		} catch (IOException e) {
			TUtilsTestNG.failForException("Could not create tgz file.", e);
		}
		return createBucketWithIndexInDirectoryAndFormat("index", bucketDir,
				BucketFormat.SPLUNK_BUCKET_TGZ);
	}
}
