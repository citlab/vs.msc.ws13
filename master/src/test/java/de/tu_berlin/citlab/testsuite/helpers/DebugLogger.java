package de.tu_berlin.citlab.testsuite.helpers;


import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;


public final class DebugLogger
{

/* public Constants */
/* ================ */

    //Headlines:
    public static final String HEADER_ID = "TestSuite.Headers";

    //Mocking-Classes:
    public static final String TUPLEMOCK_ID = "TestSuite.Mocks.Tuple";
    public static final String UDFBOLTMOCK_ID = "TestSuite.Mocks.UDFBolt";
    public static final String OCOLLMOCK_ID = "TestSuite.Mocks.OutputCollector";

    //Testing-Classes:
    public static final String OPTEST_ID = "TestSuite.OperatorTest";
    public static final String BOLTTEST_ID = "TestSuite.UDFBoltTest";


/* public Class-Enums */
/* ================== */
	
	/**
	 * The Level of Detail (in short "LoD") is set for each message and the DebugLogger itself.
	 * <p>
	 * The LoD, defined for the DebugLogger, reveals that only messages with the current-LoD, or lower, will be printed to the console.
	 */
	public enum LoD
	{
	/** Basic is the rawest LoD-Type. 
	 * If a message uses this type, it will always be printed. 
	 **/
		BASIC,
		
	/** Medium is the default assignment for the DebugLogger and all messages. 
	 * Every message for general purposes should be printed with that type. 
	 **/
		DEFAULT, 
		
	/** Detailed LoD should be used by every message, that gives a very special insight in the monitored functionality. 
	 * All messages, that will be often printed or could limit a clear view should be printed with that type. 
	 **/
		DETAILED;
	}



/* public static Methods */
/* ===================== */

    public static final Marker getBasicMarker()
    {
        Marker basicMarker = MarkerManager.getMarker(LoD.BASIC.name(), getDefaultMarker());
        return basicMarker;
    }

    public static final Marker getDefaultMarker()
    {
        Marker defaultMarker = MarkerManager.getMarker(LoD.DEFAULT.name(), getDetailedMarker());
        return defaultMarker;
    }

    public static final Marker getDetailedMarker()
    {
        Marker defaultMarker = MarkerManager.getMarker(LoD.DETAILED.name());
        return defaultMarker;
    }
}
