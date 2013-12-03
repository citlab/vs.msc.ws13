package unitTests;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import backtype.storm.Constants;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public final class MockTuple
{	

	    public static Tuple mockTickTuple() 
	    {
	        return mockTuple(Constants.SYSTEM_COMPONENT_ID, Constants.SYSTEM_TICK_STREAM_ID);
	    }
	
	    public static Tuple mockTuple(String componentId, String streamId) {
	        Tuple tuple = mock(Tuple.class);
	        when(tuple.getSourceComponent()).thenReturn(componentId);
	        when(tuple.getSourceStreamId()).thenReturn(streamId);
	        return tuple;
	    }
	    
	    public static Tuple mockTuple(List<Object> key, Values vals, Fields inputFields, Fields keyFields) 
	    {
	        Tuple tuple = mock(Tuple.class);
	        
	        when(tuple.getSourceComponent()).thenReturn("No Tick Tuple.");
	        when(tuple.getSourceStreamId()).thenReturn("No Tick Tuple.");
	        
	        when(tuple.getValues()).thenReturn(vals);
	        
	        when(tuple.select(inputFields)).thenReturn(vals);
	        when(tuple.select(keyFields)).thenReturn(key);
//	        when(tuple.getSourceComponent()).thenReturn(componentId);
//	        when(tuple.getSourceStreamId()).thenReturn(streamId);
	        return tuple;
	    }

}
