package com.splunk.shep.archiver;

import static org.testng.AssertJUnit.*;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ArchiverLoggerTest {

    PrintWriter realPrintWriter;
    StringWriter logWriter;

    @BeforeClass
    public void beforeClass() {
	realPrintWriter = ArchiverLogger.logger;
    }

    @AfterClass
    public void afterClass() {
	ArchiverLogger.logger = realPrintWriter;
    }

    @BeforeMethod
    public void beforeMethod() {
	logWriter = new StringWriter();
	ArchiverLogger.logger = new PrintWriter(logWriter);
    }

    @Test
    public void did_validArguments_expectedOutPut() {
	ArchiverLogger.did("didStuff", "happenedStuff", "expectedStuff");
	String result = logWriter.toString();
	assertTrue(
		"Loged message was " + result,
		result.matches("\\[.+?\\] did=\"didStuff\" happened=\"happenedStuff\" expected=\"expectedStuff\"\n"));

    }

    @Test
    public void done_validArguments_expectedOutPut() {
	ArchiverLogger.done("doneStuff");
	String result = logWriter.toString();
	assertTrue("Loged message was " + result,
		result.matches("\\[.+?\\] done=\"doneStuff\"\n"));
    }

    @Test
    public void will_validArguments_expectedOutPut() {
	ArchiverLogger.will("willStuff");
	String result = logWriter.toString();
	assertTrue("Loged message was " + result,
		result.matches("\\[.+?\\] will=\"willStuff\"\n"));

    }
}
