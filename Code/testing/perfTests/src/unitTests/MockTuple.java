package unitTests;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public final class MockTuple
{	

//	    public static Tuple mockTickTuple() 
//	    {
//	        return mockTuple(Constants.SYSTEM_COMPONENT_ID, Constants.SYSTEM_TICK_STREAM_ID);
//	    }

	    public static Tuple mockTuple(List<Object> key, Values vals) 
	    {
	        Tuple tuple = mock(Tuple.class);
	        when(tuple.getValues()).thenReturn(vals);
//	        when(tuple.getSourceComponent()).thenReturn(componentId);
//	        when(tuple.getSourceStreamId()).thenReturn(streamId);
	        return tuple;
	    }

}
