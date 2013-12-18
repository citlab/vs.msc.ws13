package de.tu_berlin.citlab.testsuite.mocks;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;


public final class MockOutputCollector
{	

	    public static OutputCollector mockOutputCollector() 
	    {
	    	OutputCollector oColl = mock(OutputCollector.class);
	        when(oColl.emit(anyObjectList())).thenReturn(null); //TODO: save any output in an own list.
	        //stub(oColl.ack(anyTuple())) or use spy for ack() fetching.
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
