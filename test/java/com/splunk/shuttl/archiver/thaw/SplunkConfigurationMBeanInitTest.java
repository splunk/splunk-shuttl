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
package com.splunk.shuttl.archiver.thaw;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.server.mbeans.JMXSplunkMBean;

@Test(groups = { "fast-unit" })
public class SplunkConfigurationMBeanInitTest {

	private JMXSplunkMBean mBean;
	private SplunkConfiguration splunkConfiguration;

	@BeforeMethod
	public void setUp() {
		mBean = mock(JMXSplunkMBean.class);
	}

	public void getHost_mockedHost_returnsHost() {
		when(mBean.getHost()).thenReturn("host");
		splunkConfiguration = SplunkConfiguration.createWithMBean(mBean);
		assertEquals("host", splunkConfiguration.getHost());
	}

	public void getPort_mockedPort_returnsPort() {
		when(mBean.getPort()).thenReturn("1234");
		splunkConfiguration = SplunkConfiguration.createWithMBean(mBean);
		assertEquals(1234, splunkConfiguration.getPort());
	}

	public void getUsername_mockedUsername_returnsUsername() {
		when(mBean.getUsername()).thenReturn("username");
		splunkConfiguration = SplunkConfiguration.createWithMBean(mBean);
		assertEquals("username", splunkConfiguration.getUsername());
	}

	public void getPassword_mockedPassword_returnsPassword() {
		when(mBean.getPassword()).thenReturn("password");
		splunkConfiguration = SplunkConfiguration.createWithMBean(mBean);
		assertEquals("password", splunkConfiguration.getPassword());
	}

}
