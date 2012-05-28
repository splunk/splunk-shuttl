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
package com.splunk.shuttl.testutil;

import static org.testng.AssertJUnit.*;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Be careful when using this class, because the changes are global and can be
 * hard to debug.<br/>
 * <br/>
 * Util class for running in a modified environment. Use
 * {@link TUtilsEnvironment#runInCleanEnvironment(Runnable)} and call
 * {@link TUtilsEnvironment#setEnvironment(Map)} and
 * {@link TUtilsEnvironment#setEnvironmentVariable(String, String)} from within
 * the {@link Runnable}, to use this class in a safe way.
 */
public class TUtilsEnvironment {

    /**
     * Clear the current environment. Should not be used outside the class or
     * test class.
     */
    private static void clearEnvironment() {
	Map<String, String> modifiableEnvironment = getModifiableEnvironment();
	modifiableEnvironment.clear();
    }

    private static Map<String, String> getModifiableEnvironment() {
	Class<?>[] classes = Collections.class.getDeclaredClasses();
	Map<String, String> env = System.getenv();
	for (Class<?> cl : classes) {
	    if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
		return getModifiableMap(env, cl);
	    }
	}
	return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> getModifiableMap(
	    Map<String, String> env, Class<?> cl) {
	try {
	    Field field = cl.getDeclaredField("m");
	    field.setAccessible(true);
	    Object obj = field.get(env);
	    return (Map<String, String>) obj;
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    /**
     * Set an environment variable with key and value.
     * 
     * @param key
     *            to set
     * @param value
     *            of the key
     */
    public static void setEnvironmentVariable(String key, String value) {
	Map<String, String> modifiableEnvironment = getModifiableEnvironment();
	modifiableEnvironment.put(key, value);
    }

    /**
     * @param environment
     *            to be set
     */
    public static void setEnvironment(Map<String, String> environment) {
	Map<String, String> modifiableEnvironment = getModifiableEnvironment();
	modifiableEnvironment.clear();
	modifiableEnvironment.putAll(environment);
    }

    /**
     * @return a copy of the current environment
     */
    private static Map<String, String> copyEnvironment() {
	return new HashMap<String, String>(System.getenv());
    }

    /**
     * The environment is restored after the {@link Runnable} has been run.
     * 
     * @param runnable
     *            to be run in a clean environment. <br/>
     * 
     */
    public static void runInCleanEnvironment(Runnable runnable) {
	Map<String, String> copy = TUtilsEnvironment.copyEnvironment();
	TUtilsEnvironment.clearEnvironment();
	try {
	    runnable.run();
	} finally {
	    TUtilsEnvironment.setEnvironment(copy);
	}
    }

    /**
     * Contains it's own test class so that clearEnvironment can be truly
     * private.
     */
    @Test(groups = { "fast-unit" })
    public static class UtilsEnvironmentTest {

	private Map<String, String> copyEnv;

	@BeforeClass(groups = { "fast-unit" })
	public void setUp() {
	    copyEnv = TUtilsEnvironment.copyEnvironment();
	}

	@AfterMethod(groups = { "fast-unit" })
	public void teardDown() {
	    TUtilsEnvironment.setEnvironment(copyEnv);
	    assertEnvironmentIsNotEmpty();
	}

	@Test(groups = { "fast-unit" })
	public void clearEnvironment_defaultState_emptySystemEnvironment() {
	    assertEnvironmentIsNotEmpty();
	    TUtilsEnvironment.clearEnvironment();
	    assertTrue(System.getenv().isEmpty());
	}

	public void setEnvironment_givenMapWithKeyValue_setEnvironmentToTheMap() {
	    Map<String, String> env = new HashMap<String, String>();
	    env.put("fisk", "disk");
	    TUtilsEnvironment.setEnvironment(env);
	    assertEquals(System.getenv(), env);
	}

	public void setEnvironmentVariable_keyValue_setsTheKeyToValue() {
	    TUtilsEnvironment.setEnvironmentVariable("key", "value");
	    assertEquals("value", System.getenv().get("key"));
	}

	public void copyEnvironment_defaultState_returnsACopyOfTheSystemEnvironment() {
	    Map<String, String> copy = TUtilsEnvironment.copyEnvironment();
	    TUtilsEnvironment.clearEnvironment();
	    assertTrue(!copy.isEmpty());
	    assertTrue(System.getenv().isEmpty());
	}

	public void runInCleanEnvironment_withRunnable_runRunnableWithNoEnvironmentVariables() {
	    Map<String, String> copy = TUtilsEnvironment.copyEnvironment();
	    final Boolean[] isRun = new Boolean[] { false };
	    TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

		@Override
		public void run() {
		    assertTrue(System.getenv().isEmpty());
		    isRun[0] = true;
		}
	    });
	    assertTrue(isRun[0]);
	    assertEquals(copy, System.getenv());
	}

	public void runInCleanEnvironment_runnableThrowsException_stillRestoresEnvironment() {
	    assertEnvironmentIsNotEmpty();
	    try {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

		    @Override
		    public void run() {
			throw new RuntimeException();
		    }
		});
	    } catch (RuntimeException e) {
		// Swallow exception
	    }
	    assertEnvironmentIsNotEmpty();
	}

	private void assertEnvironmentIsNotEmpty() {
	    assertFalse(System.getenv().isEmpty());
	}
    }

}
