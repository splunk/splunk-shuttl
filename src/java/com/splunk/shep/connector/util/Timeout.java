// Timeout.java
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

package com.splunk.shep.connector.util;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public abstract class Timeout implements Comparable<Timeout> {
    private Date expiryTime;

    public Timeout(Date expiryTime) {
	this.expiryTime = expiryTime;
    }

    @Override
    public int compareTo(Timeout timeout) {
	if (expiryTime.getTime() < timeout.expirationTime().getTime())
	    return -1;
	if (expiryTime.getTime() > timeout.expirationTime().getTime())
	    return 1;
	return 0;
    }

    protected Date expirationTime() {
	return expiryTime;
    }

    protected void addMS(int ms) {
	// expiryTime.
	Calendar c = new GregorianCalendar();
	c.setTime(expiryTime);
	c.add(Calendar.MILLISECOND, ms);
	expiryTime = c.getTime();
    }

    /**
     * Runs timeout task
     * 
     * @return false to remove timeout
     * @throws Exception
     */
    public boolean run() throws Exception {
	return false;
    }
}
