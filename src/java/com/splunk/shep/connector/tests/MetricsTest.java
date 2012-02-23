// Copyright (C) 2012 Splunk Inc.
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

import com.splunk.shep.connector.EventThruput;

public class MetricsTest {


    /**
     * @param args
     */
    public static void main(String[] args) {
	EventThruput et = EventThruput.getInstance();

	try {
	    // Thread.sleep(1000);

	    et.update("datagen-host3", "source1", "sourcetype1", "index1", 10);
	    et.update("datagen-host30", "source2", "sourcetype2", "index2", 20);
	    et.update("datagen-host3", "source1", "sourcetype1", "index1", 10);
	    et.update("datagen-host3", "source1", "sourcetype1", "index1", 10);
	    et.update("datagen-host33", "source3", "sourcetype3", "index3", 30);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);
	    et.update("datagen-host33", "source3", "sourcetype3", "index3", 10);
	    et.update("datagen-host3", "source1", "sourcetype1", "index1", 10);
	    et.update("datagen-host3", "source1", "sourcetype1", "index1", 10);
	    et.update("datagen-host48", "source5", "sourcetype5", "index5", 50);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);
	    et.update("datagen-host90", "source8", "sourcetype8", "index8", 80);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);
	    et.update("datagen-host96", "source9", "sourcetype9", "index9", 90);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);
	    et.update("datagen-host3", "source1", "sourcetype1", "index1", 10);
	    et.update("datagen-host30", "source2", "sourcetype2", "index2", 20);
	    et.update("datagen-host3", "source1", "sourcetype1", "index1", 10);
	    et.update("datagen-host3", "source1", "sourcetype1", "index1", 10);
	    et.update("datagen-host33", "source3", "sourcetype3", "index3", 30);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);
	    et.update("datagen-host33", "source3", "sourcetype3", "index3", 10);
	    et.update("datagen-host3", "source1", "sourcetype1", "index1", 10);
	    et.update("datagen-host3", "source1", "sourcetype1", "index1", 10);
	    et.update("datagen-host48", "source5", "sourcetype5", "index5", 50);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);
	    et.update("datagen-host90", "source8", "sourcetype8", "index8", 80);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);
	    et.update("datagen-host96", "source9", "sourcetype9", "index9", 90);
	    et.update("datagen-host38", "source4", "sourcetype4", "index4", 40);

	    et.printMetrics();

	} catch (Exception e1) {
	    e1.printStackTrace();
	}
    }
}

