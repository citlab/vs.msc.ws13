package de.tu_berlin.citlab.testsuite.mocks;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import backtype.storm.task.OutputCollector;


public final class MockOutputCollector
{	

	    public static OutputCollector mockOutputCollector() 
	    {
	    	OutputCollector oColl = mock(OutputCollector.class);
	        when(oColl.emit(anyObjectList())).thenReturn(null);
	        
	        return oColl;
	    }

		private static List<Object> anyObjectList()
		{
			// TODO Auto-generated method stub
			return null;
		}

}
