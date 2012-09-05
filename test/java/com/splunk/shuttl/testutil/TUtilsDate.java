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
package com.splunk.shuttl.testutil;

import java.util.Date;

public class TUtilsDate {

	/**
	 * @return current date in seconds which is useful for creating buckets, since
	 *         the bucket names keep the times in seconds.
	 */
	public static Date getNowWithoutMillis() {
		long timeInSeconds = System.currentTimeMillis() / 1000;
		return new Date(timeInSeconds * 1000);
	}

	/**
	 * @return later date than date. Knows that seconds will be removed if
	 *         involved with creating buckets.
	 */
	public static Date getLaterDate(Date date) {
		return new Date(date.getTime() + 10000);
	}

}
