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
package com.splunk;

import static org.testng.Assert.*;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.TUtilsIp;

@Test(groups = { "cluster-test" })
public class ClusterPeersTest {

	@Parameters(value = { "cluster.slave1.port", "cluster.master.port",
			"splunk.username", "splunk.password" })
	public void _givenSlavesGuid_getsClusterPeer(String slavePortString,
			String masterPort, String splunkUser, String splunkPass) {
		String localHostIp = TUtilsIp.getLocalHostIp();
		int slavePort = Integer.parseInt(slavePortString);

		Service slaveService = new Service(localHostIp, slavePort);
		slaveService.login(splunkUser, splunkPass);
		String slaveGuid = slaveService.getInfo().getGuid();

		Service masterService = new Service(localHostIp,
				Integer.parseInt(masterPort));
		masterService.login(splunkUser, splunkPass);
		ClusterPeers clusterPeers = new ClusterPeers(masterService);
		ClusterPeer clusterPeer = clusterPeers.get(slaveGuid);

		assertNotNull(clusterPeer);
		assertEquals(clusterPeer.getPort(), slavePort);
		assertEquals(clusterPeer.getHost(), localHostIp);
	}
}
