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
package com.splunk.shuttl.archiver.model;

import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class BucketNameTest {

	String db;
	long earliest;
	long latest;
	String index;

	private BucketName bucketName = null;

	@BeforeMethod
	public void setUp() {
		db = "db";
		earliest = 12345678;
		latest = earliest + 100;
		index = "index-1";
		bucketName = null; // To avoid boiler plate BucketName for every test.
	}

	private BucketName getBucketName() {
		return getBucketName(db, earliest, latest, index);
	}

	private BucketName getBucketName(Object db, Object earliest, Object latest,
			Object index) {
		String separator = "_";
		return new BucketName(db + separator + latest + separator + earliest
				+ separator + index);
	}

	@Test(groups = { "fast-unit" })
	public void setUp_givenEarliestLatestIndexAndDB_equalToSetUp() {
		bucketName = getBucketName();
		assertEquals(db, bucketName.getDB());
		assertEquals(earliest, bucketName.getEarliest());
		assertEquals(latest, bucketName.getLatest());
		assertEquals(index, bucketName.getIndex());
	}

	public void getDB_givenDBWithNumber_returnGivenDB() {
		String dbWithNumber = "db1";
		bucketName = getBucketName(dbWithNumber, earliest, latest, index);
		assertEquals(dbWithNumber, bucketName.getDB());
	}

	@Test(expectedExceptions = { IllegalBucketNameException.class })
	public void getDB_givenEmptyDB_throwIllegalBucketNameException() {
		bucketName = getBucketName("", earliest, latest, index);
		bucketName.getDB();
	}

	@Test(expectedExceptions = { IllegalBucketNameException.class })
	public void getDB_givenDBWithUnderscores_throwIllegalBucketNameException() {
		String db = "d_b";
		bucketName = getBucketName(db, earliest, latest, index);
		assertEquals(db, bucketName.getDB());
	}

	public void getEarliest_given0EarliestTime_0() {
		bucketName = getBucketName(db, 0, latest, index);
		assertEquals(0, bucketName.getEarliest());
	}

	@Test(expectedExceptions = { IllegalBucketNameException.class })
	public void getEarliest_givenLettersInsteadOfNumbersForEarliest_throwIllegalBucketNameException() {
		bucketName = getBucketName(db, "lettersInsteadOfNumbers", latest, index);
		bucketName.getEarliest();
	}

	@Test(expectedExceptions = { IllegalBucketNameException.class })
	public void getEarliest_givenEmptyEarliest_throwIllegalBucketNameException() {
		getBucketName(db, "", latest, index).getEarliest();
	}

	public void getLatest_given0LatestTime_0() {
		bucketName = getBucketName(db, earliest, 0, index);
		assertEquals(0, bucketName.getLatest());
	}

	@Test(expectedExceptions = { IllegalBucketNameException.class })
	public void getLatest_givenLettersForLatest_throwIllegalBucketNameException() {
		bucketName = getBucketName(db, earliest, "lettersInsteadOfNumbers", index);
		bucketName.getLatest();
	}

	@Test(expectedExceptions = { IllegalBucketNameException.class })
	public void getLatest_givenEmptyLatest_throwIllegalBucketNameException() {
		getBucketName(db, earliest, "", index).getLatest();
	}

	public void getIndex_givenDashesLettersAndNumbers_validIndex() {
		String index = "index-1332222208803";
		bucketName = getBucketName(db, earliest, latest, index);
		assertEquals(index, bucketName.getIndex());
	}

	@Test(expectedExceptions = { IllegalBucketNameException.class })
	public void getIndex_givenEmptyIndex_throwIllegalBucketNameException() {
		getBucketName(db, earliest, latest, "").getIndex();
	}

	public void getIndex_givenIndexWithUnderscores_index() {
		String index = "_index_with_underscores_";
		bucketName = getBucketName(db, earliest, latest, index);
		assertEquals(index, bucketName.getIndex());
	}

	public void getName_givenNameForString_returnString() {
		String name = "thisIsSomeString";
		BucketName bucketName = new BucketName(name);
		assertEquals(name, bucketName.getName());
	}
}
