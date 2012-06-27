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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * Bucket name with db-name, earliest time, latest time and bucket index.
 */
public class BucketName {

	private final static Logger logger = Logger.getLogger(BucketName.class);

	public static final String LEGAL_NAME_REGEX = "([A-Za-z0-9]+?)_(\\d+?)_(\\d+?)_(.+)";

	private static final int DB_GROUP = 1;
	private static final int LATEST_GROUP = 2;
	private static final int EARLIEST_GROUP = 3;
	private static final int INDEX_GROUP = 4;

	private final String name;

	/**
	 * @param name
	 *          of the bucket as a string.
	 */
	public BucketName(String name) {
		this.name = name;
	}

	/**
	 * Throws {@link IllegalBucketNameException} if name was not valid to get db.
	 * 
	 * @return db value of the {@link Bucket}'s name.
	 */
	public String getDB() {
		return getRegexValue(DB_GROUP);
	}

	/**
	 * Throws {@link IllegalBucketNameException} if name was not valid to get
	 * earliest.
	 * 
	 * @return earliest time of the {@link Bucket}'s name.
	 */
	public long getEarliest() {
		return Long.parseLong(getRegexValue(EARLIEST_GROUP));
	}

	/**
	 * Throws {@link IllegalBucketNameException} if name was not valid to get
	 * Latest.
	 * 
	 * @return time of the {@link BucketName}
	 */
	public long getLatest() {
		return Long.parseLong(getRegexValue(LATEST_GROUP));
	}

	/**
	 * Throws {@link IllegalBucketNameException} if name was not valid to get
	 * Index.
	 * 
	 * @return index of the {@link Bucket}'s name.
	 */
	public String getIndex() {
		return getRegexValue(INDEX_GROUP);
	}

	private String getRegexValue(int indexGroup) {
		throwExceptionIfNotValidRegex();
		return getRegexGroup(indexGroup);
	}

	private String getRegexGroup(int groupIndex) {
		Pattern legalPattern = Pattern.compile(LEGAL_NAME_REGEX);
		Matcher matcher = legalPattern.matcher(name);
		matcher.find();
		String group = matcher.group(groupIndex);
		return group;
	}

	private void throwExceptionIfNotValidRegex() {
		if (!Pattern.matches(LEGAL_NAME_REGEX, name)) {
			logger.debug(did("Verified legal bucket name",
					"Bucket name was not legal. Throwing IllegalBucketNameException",
					"Bucket name to be legal", "bucket_name", name,
					"legal_bucket_name_regex", LEGAL_NAME_REGEX));
			throw new IllegalBucketNameException();
		}
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
