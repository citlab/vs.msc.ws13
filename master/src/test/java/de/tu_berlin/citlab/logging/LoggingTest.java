package de.tu_berlin.citlab.logging;

import org.apache.log4j.Logger;

public class LoggingTest {
	
	private static final Logger log = Logger.getLogger(LoggingTest.class);

	public static void main(String[] args) {
		log.trace("trace");
		log.debug("debug");
		log.warn("warn");
		log.error("error");
		log.fatal("fatal");
		log.info("info");
	}
}
