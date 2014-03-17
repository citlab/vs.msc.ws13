package de.tu_berlin.citlab.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class LoggingTest {
	
	
	private static Logger log = LogManager.getLogger(LoggingTest.class);

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		log.trace("trace");
		log.debug("debug");
		log.info("info");
		log.warn("warn");
		log.error("error");
		log.fatal("fatal");
		
	}
}
