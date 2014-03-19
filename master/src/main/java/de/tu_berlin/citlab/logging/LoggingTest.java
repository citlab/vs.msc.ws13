package de.tu_berlin.citlab.logging;



public class LoggingTest {

	public static void main(String[] args) {
		PropertySetter.setLog4j2Properties("logsessionXY");
		
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
