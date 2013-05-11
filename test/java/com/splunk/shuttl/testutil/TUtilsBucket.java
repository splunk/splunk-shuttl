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
import static org.testng.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.RandomUtils;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.importexport.BucketExportControllerIntegrationTest;
import com.splunk.shuttl.archiver.importexport.BucketFileCreator;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.BucketName;
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
		return "index-" + randomBucketNumber();
	}

	private static int randomBucketNumber() {
		return RandomUtils.nextInt();
	}

	private static String randomBucketName() {
		long latest = System.currentTimeMillis() / 1000;
		long earliest = latest - (RandomUtils.nextInt(10) + 1);
		return String.format("db_%d_%d_%d", latest, earliest, randomBucketNumber());
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
		return getNameWithEarliestLatestAndBucketNumber(earliest, latest,
				randomBucketNumber());
	}

	private static String getNameWithEarliestLatestAndBucketNumber(Date earliest,
			Date latest, long bucketNumber) {
		return "db_" + toSec(latest.getTime()) + "_" + toSec(earliest.getTime())
				+ "_" + bucketNumber;
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
	public static LocalBucket createBucketWithName(String name) {
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

	public static LocalBucket createRealReplicatedBucket(String index,
			File parent, String guid) {
		LocalBucket realBucket = TUtilsBucket.createRealBucket();
		String replicatedBucketName = replicatedBucketName(realBucket, guid);
		File bucketDir = realBucket.getDirectory();
		File renamedDir = new File(parent, replicatedBucketName);
		int filesInBucket = bucketDir.listFiles().length;
		assertTrue(bucketDir.renameTo(renamedDir));
		assertEquals(filesInBucket, renamedDir.listFiles().length);
		try {
			return new LocalBucket(renamedDir, index, realBucket.getFormat());
		} catch (Exception e) {
			TUtilsTestNG.failForException("Could not create bucket", e);
			return null;
		}
	}

	/**
	 * @return bucket that has the bucket name of a bucket created with Splunk
	 *         clustering bucket replication.
	 */
	public static LocalBucket createReplicatedBucket(String index, File parent,
			String guid) {
		Bucket bucket = createBucket();
		String finalBucketName = replicatedBucketName(bucket, guid);

		File dir = createDirectoryInParent(parent, finalBucketName);
		return createBucketWithIndexInDirectory(index, dir);
	}

	private static String replicatedBucketName(Bucket bucket, String guid) {
		String replicatedBucketName = bucket.getName().replaceFirst("db", "rb");
		return replaceEverythingAfterEarliestTimeWithIndexAndGuid(
				replicatedBucketName, Math.abs(new Random().nextLong()), guid);
	}

	public static String replaceEverythingAfterEarliestTimeWithIndexAndGuid(
			String bucketName, long newBucketIndex, String guid) {
		long bucketNumber = new BucketName(bucketName).getBucketNumber();
		int idx = bucketName.lastIndexOf("" + bucketNumber);
		String removedIndex = bucketName.substring(0, idx);
		return removedIndex + newBucketIndex + "_" + guid;
	}

	public static LocalBucket createReplicatedBucket() {
		return createReplicatedBucket(randomIndexName(), createDirectory(),
				randomIndexName());
	}

	public static LocalBucket createReplicatedBucket(String guid) {
		return createReplicatedBucket(randomIndexName(), createDirectory(), guid);
	}

	public static LocalBucket createBucketWithIndex(String index) {
		return TUtilsBucket.createBucketInDirectoryWithIndex(createDirectory(),
				index);
	}

	public static LocalBucket createBucketWithBucketNumber(int i) {
		Bucket b = bucketWithRandomName();
		return createBucketWithBucketNumber(i, randomIndexName(), b.getLatest(),
				b.getEarliest());
	}

	private static Bucket bucketWithRandomName() {
		try {
			return new Bucket(null, null, randomBucketName(), null, null);
		} catch (Exception e) {
			TUtilsTestNG.failForException(null, e);
			return null;
		}
	}

	public static LocalBucket createBucketWithBucketNumber(int bucketNumber,
			String index, Date latest, Date earliest) {
		return createBucketWithIndexAndName(
				index,
				getNameWithEarliestLatestAndBucketNumber(earliest, latest, bucketNumber));
	}
}
