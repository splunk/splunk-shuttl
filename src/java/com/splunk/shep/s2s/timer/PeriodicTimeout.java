// PeriodicTimeout.java
//
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

package com.splunk.shep.s2s.timer;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public abstract class PeriodicTimeout extends Timeout {
    private int periodInMs;

    public PeriodicTimeout(int periodInMs) {
	super(getNextExiperyDate(periodInMs));
	this.periodInMs = periodInMs;
    }

    public boolean run() throws Exception {
	if (runPeriodicTask() == false)
	    return false;

	addMS(periodInMs);
	return true;
    }

    private static Date getNextExiperyDate(int periodMs) {
	Calendar c = new GregorianCalendar();
	c.add(Calendar.MILLISECOND, periodMs);
	Date expiryTime = c.getTime();
	return expiryTime;
    }

    public abstract boolean runPeriodicTask() throws Exception;
}
