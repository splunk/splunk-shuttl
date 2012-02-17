// MetricsManager.java
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

package com.splunk.shep.s2s.forwarder;

import com.splunk.shep.connector.util.PeriodicTimeout;

public class HDFSFileTimeout extends PeriodicTimeout {
    private HDFSSink fileMgr = null;

    public HDFSFileTimeout(int periodInMs) {
	super(periodInMs);
    }

    public void setFileIO(HDFSSink fileIO) {
	fileMgr = fileIO;
    }

    @Override
    public boolean runPeriodicTask() throws Exception {
	if (fileMgr != null)
	    fileMgr.checkOpenDuration();
	return true;
    }
}
