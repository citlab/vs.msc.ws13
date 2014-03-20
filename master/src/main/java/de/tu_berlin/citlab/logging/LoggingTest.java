package de.tu_berlin.citlab.logging;

public class LoggingTest {

	public static void main(String[] args) {
		/*
		 * !!! IMPORTANT !!!
		 * ONLY TEST EXACTLY ONE OF THESE PER CALL!
		 * ANY ATTEMPT TO TEST BOTH IN A SINGLE RUN, WILL FAIL!
		 */
		testOnlyConsole();
		//testBothConsoleAndDatabase();
	}
	
	private static void testOnlyConsole() {
		testAllLoggers();
	}
	
	private static void testBothConsoleAndDatabase() {
		LoggingConfigurator.activateDataBaseLogger("logsessionXY");
		testAllLoggers();
	}
	
	private static void testAllLoggers() {
		SLF4JLoggerTest();
		Log4J12LoggerTest();
		Log4J2LoggerTest();
	}

	private static void SLF4JLoggerTest() {
		org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LoggingTest.class + "->slf4j");
		// log levels
		log.trace("trace");
		log.debug("debug");
		log.info("info");
		log.warn("warn");
		log.error("error");
		// exceptions
		log.error("kaputt", new RuntimeException("panic!!!"));
		// marker
		log.info(org.slf4j.MarkerFactory.getMarker("marker1"), "this is a marked message");
		// getting owned by cat-smileys... Apparently I'm not able to configure mysql to use utf8mb4...
		// byte[] bytes = { (byte) 0xf0, (byte) 0x9F, (byte) 0x99, (byte) 0x8C };
		// log.info(new String(bytes));
	}
	
	
	private static void Log4J12LoggerTest() {
		org.apache.log4j.Logger log = org.apache.log4j.LogManager.getLogger(LoggingTest.class.getName() + "->log4j1.2");
		// log levels
		log.trace("trace");
		log.debug("debug");
		log.info("info");
		log.warn("warn");
		log.error("error");
		// exceptions
		log.error("kaputt", new RuntimeException("panic!!!"));
		// marker
		// not existent in log4j 1.2
	}
	
	private static void Log4J2LoggerTest() {
		org.apache.logging.log4j.Logger log = org.apache.logging.log4j.LogManager.getLogger(LoggingTest.class.getName() + "->log4j2");
		// log levels
		log.trace("trace");
		log.debug("asd");
		log.info("info");
		log.warn("warn");
		log.error("error");
		// exceptions
		log.error("kaputt", new RuntimeException("panic!!!"));
		// marker
		log.info(org.apache.logging.log4j.MarkerManager.getMarker("marker1"), "this is a marked message");
	}


}
