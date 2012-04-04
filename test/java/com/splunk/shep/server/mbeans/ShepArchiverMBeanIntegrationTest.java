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
package com.splunk.shep.server.mbeans;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.splunk.shep.server.ShepJettyServer;
import com.splunk.shep.server.mbeans.util.MBeanUtils;

/**
 * Test Archiver MBean integration
 */
@Test(groups = { "slow-unit" })
public class ShepArchiverMBeanIntegrationTest {

    /**
     * Tests that the Archiver MBean has been properly set up and is accessible
     * through a proxy.
     * 
     * Note: this test is currently disabled since
     * {@link ShepJettyServer#main(String[])} relies on hard-coded paths
     * relative to bin. This means that the test can not be run using TestNG
     * unless the paths are modified (which breaks normal builds).
     */
    @Test(groups = { "slow-unit" }, enabled = false)
    public void _givenMBeanRegisteredInJetty_getsInstanceFromMBeanManager()
	    throws Exception {
	ShepJettyServer.main(null);
	ShepArchiverMBean proxy = MBeanUtils.getMBeanInstance(
		ShepArchiverMBean.OBJECT_NAME,
		ShepArchiverMBean.class);
	assertNotNull(proxy);
    }
}
