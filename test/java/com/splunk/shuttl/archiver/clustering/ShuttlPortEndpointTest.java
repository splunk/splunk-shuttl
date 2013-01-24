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
package com.splunk.shuttl.archiver.clustering;

import static org.testng.Assert.*;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Service;

@Test(groups = { "end-to-end" })
public class ShuttlPortEndpointTest {

	@Parameters(value = { "splunk.host", "splunk.mgmtport", "shuttl.port" })
	public void getShuttlPort_givenSplunkIndexer_getsShuttlPort(
			String splunkHost, String splunkPort, String shuttlPort) {
		Service service = new Service(splunkHost, Integer.parseInt(splunkPort));
		ShuttlPortEndpoint shuttlPortEntity = ShuttlPortEndpoint.create(service);

		int actualShuttlPort = shuttlPortEntity.getShuttlPort();

		assertEquals(actualShuttlPort, Integer.parseInt(shuttlPort));
	}
}
