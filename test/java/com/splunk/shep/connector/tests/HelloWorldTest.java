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

package com.splunk.shep.connector.tests;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.connector.EventEmitter;

/*
 * HelloWorldTest
 * 
 * This is a trivial test of the code to demo how to get started with testng.
 * It is mostly for demonstrating we have testNG.
 * We'll need to add more tests over time.
 *
 */

public class HelloWorldTest {

    @BeforeMethod(groups = { "fast" })
    // This setUp won't be run without the groups argument,
    // like so: @BeforeClass(groups = { "fast" })
    public void setUp() {
	System.out.println("Running Setup");
	// code that will be invoked when this test is instantiated
    }

    /*
     * This just checks that when initialized, the EventEmitter object has the
     * two fields set.
     */
    @Test(groups = { "fast" })
    public void eventEmitterHelloTest() {
	System.out.println("Running Event Emitter test");
	int targetPort = 8888;
	String targetIP = new String("0.0.0.0");
	EventEmitter emitter = null;
	try {
	    emitter = new EventEmitter(targetPort, targetIP);
	    assertEquals(targetIP, emitter.getIp());
	    assertEquals(targetPort, emitter.getPort());
	    assertNotNull(emitter);
	} catch (Exception e) {
	    fail();
	}

    }

    @Test(groups = { "fast" })
    public void fooTest() {
	System.out.println("Running foo test");
    }

}
