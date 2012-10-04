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

import com.splunk.shuttl.archiver.util.GroupRegex;
import com.splunk.shuttl.archiver.util.IllegalRegexGroupException;

/**
 * Bucket name with db-name, earliest time, latest time and bucket index.
 */
public class BucketName {

	public static final String LEGAL_NAME_REGEX = "([A-Za-z0-9]+?)_(\\d+?)_(\\d+?)_(.+)";

	private static final int DB_GROUP = 1;
	private static final int LATEST_GROUP = 2;
	private static final int EARLIEST_GROUP = 3;
	private static final int INDEX_GROUP = 4;

	private final String name;

	private GroupRegex groupRegex;

	/**
	 * @param name
	 *          of the bucket as a string.
	 */
	public BucketName(String name) {
		this.name = name;
		this.groupRegex = new GroupRegex(LEGAL_NAME_REGEX, name);
	}

	/**
	 * Throws {@link IllegalRegexGroupException} if name was not valid to get db.
	 * 
	 * @return db value of the {@link Bucket}'s name.
	 */
	public String getDB() {
		return groupRegex.getValue(DB_GROUP);
	}

	/**
	 * Throws {@link IllegalRegexGroupException} if name was not valid to get
	 * earliest.
	 * 
	 * @return earliest time of the {@link Bucket}'s name.
	 */
	public long getEarliest() {
		return Long.parseLong(groupRegex.getValue(EARLIEST_GROUP));
	}

	/**
	 * Throws {@link IllegalRegexGroupException} if name was not valid to get
	 * Latest.
	 * 
	 * @return time of the {@link BucketName}
	 */
	public long getLatest() {
		return Long.parseLong(groupRegex.getValue(LATEST_GROUP));
	}

	/**
	 * Throws {@link IllegalRegexGroupException} if name was not valid to get
	 * Index.
	 * 
	 * @return index of the {@link Bucket}'s name.
	 */
	public String getIndex() {
		return groupRegex.getValue(INDEX_GROUP);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return name;
	}

	/**
	 * @return full name of the {@link BucketName}.
	 */
	public String getName() {
		return name;
	}

}
