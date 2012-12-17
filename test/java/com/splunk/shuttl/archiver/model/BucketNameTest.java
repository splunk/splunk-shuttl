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

import com.splunk.shuttl.archiver.model.BucketName.IllegalBucketNameException;
import com.splunk.shuttl.archiver.util.IllegalRegexGroupException;

@Test(groups = { "fast-unit" })
public class BucketNameTest {

	String db;
	long earliest;
	long latest;
	String index;
	String guid;

	private BucketName bucketName = null;
	private String separator;

	@BeforeMethod
	public void setUp() {
		db = "db";
		earliest = 12345678;
		latest = earliest + 100;
		index = "index-1";
		guid = "guid";
		bucketName = null; // To avoid boiler plate BucketName for every test.

		separator = "_";
	}

	private BucketName getBucketName() {
		return getBucketName(db, earliest, latest, index, guid);
	}

	private BucketName getBucketName(Object db, Object earliest, Object latest,
			Object index, Object guid) {
		return new BucketName(db + separator + latest + separator + earliest
				+ separator + index + separator + guid);
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
		bucketName = getBucketName(dbWithNumber, earliest, latest, index, guid);
		assertEquals(dbWithNumber, bucketName.getDB());
	}

	@Test(expectedExceptions = { IllegalRegexGroupException.class })
	public void getDB_givenEmptyDB_throwIllegalBucketNameException() {
		bucketName = getBucketName("", earliest, latest, index, guid);
		bucketName.getDB();
	}

	@Test(expectedExceptions = { IllegalBucketNameException.class })
	public void getDB_givenDBWithUnderscores_throwIllegalBucketNameException() {
		String db = "d_b";
		bucketName = getBucketName(db, earliest, latest, index, guid);
		assertEquals(db, bucketName.getDB());
	}

	public void getEarliest_given0EarliestTime_0() {
		bucketName = getBucketName(db, 0, latest, index, guid);
		assertEquals(0, bucketName.getEarliest());
	}

	@Test(expectedExceptions = { IllegalRegexGroupException.class })
	public void getEarliest_givenLettersInsteadOfNumbersForEarliest_throwIllegalBucketNameException() {
		bucketName = getBucketName(db, "lettersInsteadOfNumbers", latest, index,
				guid);
		bucketName.getEarliest();
	}

	@Test(expectedExceptions = { IllegalRegexGroupException.class })
	public void getEarliest_givenEmptyEarliest_throwIllegalBucketNameException() {
		getBucketName(db, "", latest, index, guid).getEarliest();
	}

	public void getLatest_given0LatestTime_0() {
		bucketName = getBucketName(db, earliest, 0, index, guid);
		assertEquals(0, bucketName.getLatest());
	}

	@Test(expectedExceptions = { IllegalRegexGroupException.class })
	public void getLatest_givenLettersForLatest_throwIllegalBucketNameException() {
		bucketName = getBucketName(db, earliest, "lettersInsteadOfNumbers", index,
				guid);
		bucketName.getLatest();
	}

	@Test(expectedExceptions = { IllegalRegexGroupException.class })
	public void getLatest_givenEmptyLatest_throwIllegalBucketNameException() {
		getBucketName(db, earliest, "", index, guid).getLatest();
	}

	public void getIndex_givenDashesLettersAndNumbers_validIndex() {
		String index = "index-1332222208803";
		bucketName = getBucketName(db, earliest, latest, index, guid);
		assertEquals(index, bucketName.getIndex());
	}

	@Test(expectedExceptions = { IllegalRegexGroupException.class })
	public void getIndex_givenEmptyIndex_throwIllegalBucketNameException() {
		getBucketName(db, earliest, latest, "", guid).getIndex();
	}

	@Test(expectedExceptions = { IllegalBucketNameException.class })
	public void getIndex_givenIndexWithUnderscores_throws() {
		String index = "_index_with_underscores_";
		bucketName = getBucketName(db, earliest, latest, index, guid);
		bucketName.getIndex();
	}

	public void getName_givenNameForString_returnString() {
		String name = "this_string_is_legal";
		BucketName bucketName = new BucketName(name);
		assertEquals(name, bucketName.getName());
	}

	public void getGuid_givenGuid_returnGuid() {
		BucketName bucketName = getBucketName(db, earliest, latest, index, guid);
		assertEquals(guid, bucketName.getGuid());
	}

	@Test(expectedExceptions = { IllegalRegexGroupException.class })
	public void getGuid_bucketNameWithoutGuid_throws() {
		getBucketName(db, earliest, latest, index, "").getGuid();
	}

	public void constructor_null_doesNothing() {
		new BucketName(null);
	}
}
