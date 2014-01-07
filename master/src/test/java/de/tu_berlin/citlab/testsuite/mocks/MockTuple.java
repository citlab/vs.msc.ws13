package de.tu_berlin.citlab.testsuite.mocks;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
	        when(tuple.toString()).thenAnswer(new Answer<String>(){
	        	
	        	public String answer(InvocationOnMock invocation)
						throws Throwable {
	        		String toString = MockTuple.toComponentTupleString((Tuple) invocation.getMock());
					return toString;
				}
	        });
	        return tuple;
	    }
	    
	    public static Tuple mockTuple(List<Object> key, Values vals, Fields inputFields, Fields keyFields) 
	    {
	        Tuple tuple = mock(Tuple.class);
	        
	        when(tuple.getSourceComponent()).thenReturn("No Tick Tuple.");
	        when(tuple.getSourceStreamId()).thenReturn("No Tick Tuple.");
	        
	        when(tuple.getValues()).thenReturn(vals);
	        when(tuple.getFields()).thenReturn(keyFields, inputFields);
	        when(tuple.select(inputFields)).thenReturn(vals);
	        when(tuple.select(keyFields)).thenReturn(key);
	        
	        when(tuple.toString()).thenAnswer(new Answer<String>(){

				public String answer(InvocationOnMock invocation)
						throws Throwable {
					String toString = MockTuple.toValueTupleString((Tuple) invocation.getMock());
					return toString;
				}
	        	
	        });

	        return tuple;
	    }
	    
	    private static String toComponentTupleString(Tuple mockTuple)
	    {
	    	//If the mockTuple is a TickTuple, simply return this as a String-output:
	    	if(mockTuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)){
	    		return "TickTuple";
	    	}
	    	else{
	    		String tupleString = "Component-String "+
	    				 			  "(Comp-ID: "+ mockTuple.getSourceComponent() +", "+
	    							  "Stream-ID: "+ mockTuple.getSourceStreamId() +")";
	    		return tupleString;
	    	}
	    	
	    }
	    
	    private static String toValueTupleString(Tuple mockTuple)
	    {
	    	String tupleString = "Tuple <Vals: (";
	    	int valSize = mockTuple.getValues().size();
	    	for(int n = 0 ; n < valSize ; n++){
	    		String actVal = (String) mockTuple.getValues().get(n);
	    		tupleString += actVal;
	    		if(n+1 != valSize)
	    			tupleString += ", ";
	    	}
	    	tupleString += "), Key: (";
	    	Fields keyFields = mockTuple.getFields();
	    	List<Object> key = mockTuple.select(keyFields);
	    	
	    	for(int n = 0 ; n < key.size() ; n++){
	    		Integer actInt = (Integer) key.get(n);
	    		tupleString += Integer.toString(actInt);
	    		if(n+1 != key.size())
	    			tupleString += ", ";
	    	}
	    	tupleString += ")>";
	    	
	    	return tupleString;
	    }

}
