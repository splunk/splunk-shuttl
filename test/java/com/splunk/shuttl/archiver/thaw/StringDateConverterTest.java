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
package com.splunk.shuttl.archiver.thaw;

import static org.testng.Assert.*;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class StringDateConverterTest {

	Calendar calendar = Calendar.getInstance();

	@Test(groups = { "fast-unit" })
	public void convert_unixTime_getDateRepresentedByUnixTime() {
		Date date = new Date();
		String toConvert = date.getTime() + "";
		Date actual = StringDateConverter.convert(toConvert);
		assertEquals(date, actual);
	}

	@Test(groups = { "fast-unit" })
	public void convert_yearMonthDate_date() {
		// Month is 0-based 2012-01-01_00:00:00
		Date expected = new GregorianCalendar(2012, 0, 1, 0, 0, 0).getTime();

		Date actual = StringDateConverter.convert("2012-01-01_00:00:00");

		assertEquals(actual, expected);
	}
}
