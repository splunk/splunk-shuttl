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
package com.splunk.shuttl.archiver.archive;

import static java.util.Arrays.*;
import static org.testng.AssertJUnit.*;

import java.util.HashMap;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class ShellExecutorTest {

    private ShellExecutor shellExecutor;
    private HashMap<String, String> env;

    @BeforeMethod
    public void setUp() {
	shellExecutor = new ShellExecutor(Runtime.getRuntime());
	env = new HashMap<String, String>();
    }

    public void executeCommand_givenEnvironmentVariable_echoThatEnvVar() {
	env.put("SHELL_EXECUTOR", "foo");
	String[] command = new String[] { "sh", "-c", "echo ${SHELL_EXECUTOR}" };
	shellExecutor.executeCommand(env, asList(command));
	List<String> out = shellExecutor.getStdOut();
	assertEquals(1, out.size());
	assertEquals("foo", out.get(0));
    }
}
