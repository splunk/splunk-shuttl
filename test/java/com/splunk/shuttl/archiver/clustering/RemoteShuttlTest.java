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

import java.io.File;

import javax.management.InstanceNotFoundException;

import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.server.mbeans.ShuttlArchiver;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "end-to-end" })
public class RemoteShuttlTest {

	private RemoteShuttl remoteShuttl;

	@Parameters(value = { "shuttl.host", "shuttl.port", "shuttl.conf.dir" })
	public void getServerName_givenHostNameandPort_callsRestEndpointForGettingServerName(
			final String shuttlHost, final String shuttlPort, String shuttlConfDir) {

		TUtilsMBean.runWithRegisteredMBeans(new File(shuttlConfDir),
				new Runnable() {

					@Override
					public void run() {
						remoteShuttl = new RemoteShuttl(new DefaultHttpClient());
						String actualServerName = remoteShuttl.getServerName(shuttlHost,
								Integer.parseInt(shuttlPort));
						String expectedServerName = getConfiguredServerName();
						assertEquals(expectedServerName, actualServerName);
					}

					private String getConfiguredServerName() {
						try {
							return ShuttlArchiver.getMBeanProxy().getServerName();
						} catch (InstanceNotFoundException e) {
							TUtilsTestNG.failForException(
									"Could not get ShuttlArchiverMBean", e);
							return null;
						}
					}
				});
	}
}
