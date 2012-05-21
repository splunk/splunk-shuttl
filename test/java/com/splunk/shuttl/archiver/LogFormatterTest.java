package com.splunk.shuttl.archiver;

import static com.splunk.shuttl.archiver.LogFormatter.*;
import static org.testng.AssertJUnit.*;

import java.util.Date;

import org.testng.Reporter;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Test(groups = { "fast-unit" })
public class LogFormatterTest {

    String assertFailedMessageStart;

    @BeforeSuite
    public void beforeClass() {
    }

    @AfterSuite
    public void afterClass() {
    }

    @BeforeMethod
    public void beforeMethod() {
	assertFailedMessageStart = "Logged message was ";
    }

    @Test(groups = { "fast-unit" })
    public void did_validArguments_expectedOutput() {
	String result = did("didStuff", "happenedStuff", "expectedStuff");
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("did=\"didStuff\" happened=\"happenedStuff\" expected=\"expectedStuff\""));
    }

    public void did_validArgumentsNoExpected_expectedOutput() {
	String result = did("didStuff", "happenedStuff", null);
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("did=\"didStuff\" happened=\"happenedStuff\""));
    }

    public void did_withAdditionalKeyValues_expectedOutput() {
	String result = did("didStuff", "happenedStuff", "expectedStuff",
		"btw",
		"200");
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("did=\"didStuff\" happened=\"happenedStuff\" expected=\"expectedStuff\" btw=\"200\""));
    }

    public void did_withAdditionalKeyValuesNoExpected_expectedOutput() {
	String result = did("didStuff", "happenedStuff", null, "btw", "200");
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("did=\"didStuff\" happened=\"happenedStuff\" btw=\"200\""));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void did_ilegalNumberOfArguments_illegalArgumentException() {
	did("didStuff", "happenedStuff", "expectedStuff",
		"more stuff");
    }

    public void did_nonObjectArguments_expectedOutput() {
	Date currentDate = new Date();
	Reporter.log("Hejsan");
	// Test
	String result = did(currentDate, "btw", 200, "btw", 5.6f);

	// Verify
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("did=\"" + currentDate.toString()
			+ "\" happened=\"btw\" expected=\"200\" btw=\"5.6\""));
    }

    public void happened_noAdditionalKeyValues_expectedOutput() {
	String result = happened("happenedStuff");
	assertTrue(assertFailedMessageStart + result,
		result.matches("happened=\"happenedStuff\""));
    }

    public void happened_withAdditionalKeyValues_expectedOutput() {
	String result = happened("happenedStuff", "btw", "200");
	assertTrue(assertFailedMessageStart + result,
		result.matches("happened=\"happenedStuff\" btw=\"200\""));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void happened_ilegalNumberOfArguments_illegalArgumentException() {
	happened("happenedStuff", "btw");
    }

    public void happened_nonObjectArguments_expectedOutput() {
	Date currentDate = new Date();

	// Test
	String result = happened(currentDate, "btw", 200);

	// Verify
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("happened=\"" + currentDate.toString()
			+ "\" btw=\"200\""));
    }

    public void done_noAdditionalKeyValues_expectedOutput() {
	String result = done("doneStuff");
	assertTrue(assertFailedMessageStart + result,
		result.matches("done=\"doneStuff\""));
    }

    public void done_withAdditionalKeyValues_expectedOutput() {
	String result = done("doneStuff", "btw", "200");
	assertTrue(assertFailedMessageStart + result,
		result.matches("done=\"doneStuff\" btw=\"200\""));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void done_ilegalNumberOfArguments_illegalArgumentException() {
	done("doneStuff", "btw");
    }

    public void done_nonObjectArguments_expectedOutput() {
	Date currentDate = new Date();

	// Test
	String result = done(currentDate, "btw", 200);

	// Verify
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("done=\"" + currentDate.toString()
			+ "\" btw=\"200\""));
    }

    public void will_validArguments_expectedOutput() {
	String result = will("willStuff");
	assertTrue(assertFailedMessageStart + result,
		result.matches("will=\"willStuff\""));

    }

    public void will_withAdditionalKeyValues_expectedOutput() {
	String result = will("willStuff", "args", "stuff stuff stuff");
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("will=\"willStuff\" args=\"stuff stuff stuff\""));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void will_ilegalNumberOfArguments_illegalArgumentException() {
	will("willStuff", "btw");
    }

    public void will_nonObjectArguments_expectedOutput() {
	Date currentDate = new Date();

	// Test
	String result = will("willStuff", currentDate, 5);

	// Verify
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("will=\"willStuff\" " + currentDate.toString()
			+ "=\"5\""));
    }

    private boolean thatLogContainsObject(String log, Object object) {
	return log.contains(object.toString());
    }

    private String warnWithKeyValues(Object... keyValues) {
	return warn("did", "happened", "result", keyValues);
    }

    public void warn_givenEmptyMessage_stringContainsNothingExceptTheStandardFormat() {
	String result = warnWithKeyValues();
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("did=\"did\" happened=\"happened\" result=\"result\""));
    }

    public void warn_givenDidHappenedAndResultsAsStrings_containsObjectToString() {
	Object did = new Object();
	Object happened = new Object();
	Object result = new Object();
	String resultLog = warn(did, happened, result);
	assertTrue(thatLogContainsObject(resultLog, did.toString()));
	assertTrue(thatLogContainsObject(resultLog, happened.toString()));
	assertTrue(thatLogContainsObject(resultLog, result.toString()));
    }

    public void warn_givenMessageAndSingleKeyValue_containsMessageAndKeyValue() {
	Object key1 = new Object();
	Object value1 = new Object();
	Object key2 = new Object();
	Object value2 = new Object();
	String result = warnWithKeyValues(key1, value1, key2, value2);
	assertTrue(thatLogContainsObject(result, key1.toString()));
	assertTrue(thatLogContainsObject(result, value1.toString()));
	assertTrue(thatLogContainsObject(result, key2.toString()));
	assertTrue(thatLogContainsObject(result, value2.toString()));
    }

    @Test(expectedExceptions = { IllegalArgumentException.class })
    public void warn_givenMessageAndKeyWithoutRespectiveValue_throwIllegalArgumentException() {
	Object keyWithoutRespectiveValue = new Object();
	warnWithKeyValues(keyWithoutRespectiveValue);
    }

}
