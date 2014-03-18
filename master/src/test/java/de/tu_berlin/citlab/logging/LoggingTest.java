package de.tu_berlin.citlab.logging;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

public class LoggingTest {
	
	private static Logger log = LoggerFactory.getLogger(LoggingTest.class);

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		// log levels
		log.trace("trace");
		log.debug("debug");
		log.info("info");
		log.warn("warn");
		log.error("error");
		// exceptions
		log.error("kaputt", new RuntimeException("panic!!!"));
		// marker
		log.info(MarkerFactory.getMarker("marker1"), "this is a marked message");
	}
}
