package de.tu_berlin.citlab.testsuite.mocks;


import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;


public final class MockOutputCollector
{	
		private static List<List<Object>> output;
		
	    public static OutputCollector mockOutputCollector() 
	    {
	    	OutputCollector oColl = mock(OutputCollector.class);
	    	output = new ArrayList<List<Object>>();
	    	
	        when(oColl.emit(anyObjectList())).thenReturn(null); //TODO: save any output in output list.
	        doNothing().when(oColl).ack(anyTuple());
	        return oColl;
	    }

		private static Tuple anyTuple()
		{
			// TODO Auto-generated method stub
			return null;
		}

		private static List<Object> anyObjectList()
		{
			// TODO Auto-generated method stub
			return null;
		}

}
