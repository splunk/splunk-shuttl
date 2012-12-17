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
package com.splunk.shuttl.archiver.util;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.model.BucketName;

/**
 * Takes a regex with groups (.*) and a value, and returns the value for a regex
 * at a specific group index. @see {@link BucketName}
 */
public class GroupRegex {

	private static final Logger logger = Logger.getLogger(GroupRegex.class);
	private final String regex;
	private final String value;

	public GroupRegex(String regex, String value) {
		this.regex = regex;
		this.value = value;
	}

	/**
	 * @return value from the regex group by index.
	 */
	public String getValue(int groupIndex) {
		throwExceptionIfNotValidRegex(groupIndex);
		try {
			return getRegexGroup(groupIndex);
		} catch (IndexOutOfBoundsException e) {
			throw new IllegalRegexGroupException(e);
		}
	}

	private String getRegexGroup(int groupIndex) {
		Pattern legalPattern = Pattern.compile(regex);
		Matcher matcher = legalPattern.matcher(value);
		matcher.find();
		return matcher.group(groupIndex);
	}

	private void throwExceptionIfNotValidRegex(int groupIndex) {
		if (!Pattern.matches(regex, value)) {
			logger.debug(did("Verified legal bucket name",
					"Bucket name was not legal. Throwing IllegalBucketNameException",
					"Bucket name to be legal", "bucket_name", value,
					"legal_bucket_name_regex", regex));
			throw new IllegalRegexGroupException();
		}
	}
}
