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
package com.splunk.shuttl.archiver.endtoend;

import org.testng.annotations.BeforeSuite;

public class EndToEndTestSetUp {

    @BeforeSuite(groups = { "end-to-end" })
    public void setUp() {
	System.out.println("Waiting for server to start "
		+ "before running end-to-end tests.");
	waitForServerToStart();
    }

    private void waitForServerToStart() {
	try {
	    Thread.sleep(3000);
	} catch (InterruptedException e) {
	}
    }
}
