package de.tu_berlin.citlab.testsuite.mocks;


import static org.mockito.Mockito.*;

import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger.LoD;
import de.tu_berlin.citlab.testsuite.helpers.DebugPrinter;
import backtype.storm.Constants;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public final class TupleMock
{	
		private final static String TAG = "TupleMock";
	
	    public static Tuple mockTickTuple() 
	    {
	    	DebugLogger.log_Message(LoD.DETAILED, TAG, "Created Tick-Tuple Mock.", 
	    							"SYS_COMP_ID: "+ Constants.SYSTEM_COMPONENT_ID,
	    							"SYS_TICK-STREAM_ID: "+ Constants.SYSTEM_TICK_STREAM_ID);
	        return TupleMock.mockSourceTuple(Constants.SYSTEM_COMPONENT_ID, Constants.SYSTEM_TICK_STREAM_ID);
	    }
	
	    public static Tuple mockSourceTuple(String componentID, String streamID) 
	    {
	    	DebugLogger.log_Message(LoD.DETAILED, TAG, "Created Source-Tuple Mock.", 
					"SYS_COMP_ID: "+ componentID,
					"SYS_TICK-STREAM_ID: "+ streamID);
	        return TupleMock.mockTuple(componentID, streamID, null, KeyConfigFactory.DefaultKey());
	    }
	    
	    
	    public static Tuple mockTuple(Values vals)
	    {
	    	DebugLogger.log_Message(LoD.DETAILED, TAG, "Created Tuple Mock by DefaultKey.", 
					"Vals: "+ DebugPrinter.toString(vals),
					"IKeyConfig: "+ "DEFAULT-Key");
	    	return TupleMock.mockTuple(null, null, vals, KeyConfigFactory.DefaultKey());
	    }
	    
	    public static Tuple mockTupleBySource(String componentID, Values vals)
	    {
	    	DebugLogger.log_Message(LoD.DETAILED, TAG, "Created Tuple Mock by Source.", 
					"Vals: "+ DebugPrinter.toString(vals),
					"SYS_COMP_ID: "+ componentID);
	    	return TupleMock.mockTuple(componentID, null, vals, KeyConfigFactory.BySource());
	    }
	    
	    public static Tuple mockTupleByFields(Values vals, Fields fields)
	    {
	    	DebugLogger.log_Message(LoD.DETAILED, TAG, "Created Tuple Mock by Fields.", 
					"Vals: "+ DebugPrinter.toString(vals),
					"Fields: "+ DebugPrinter.toString(fields));
	    	return TupleMock.mockTuple(null, null, vals, KeyConfigFactory.ByFields(fields), (String[]) fields.toList().toArray());
	    }
	    
	    /**
	     * The most general Tuple Mockup. This Mock-Up is implementing the functionality, 
	     * while all other Mock-Up calls are just convenient methods that are re-directing to this method.
	     * @param componentID
	     * @param streamID
	     * @param vals
	     * @param keyConfig
	     * @return The Mockup as a {@link Tuple}
	     */
	    public static Tuple mockTupleCompletely(String componentID, String streamID, Values vals, IKeyConfig keyConfig, String... keyFields)
	    {
	    	DebugLogger.log_Message(LoD.DETAILED, TAG, "Created Tuple Mock by complete assignment.", 
					"Vals: "+ DebugPrinter.toString(vals),
					"SYS_COMP_ID: "+ componentID,
					"SYS_STREAM_ID: "+ streamID);
	    	return TupleMock.mockTuple(componentID, streamID, vals, keyConfig, keyFields);
	    }
	    

	    
	    private static Tuple mockTuple(String componentID, String streamID, Values vals, IKeyConfig keyConfig, String... keyFields) 
	    {
	        Tuple tuple = mock(Tuple.class);
	        
	        
	        class IsAnyFieldsSelector extends ArgumentMatcher<Fields>
	 	    {
	 			@Override
	 			public boolean matches(Object argument){
	 				if(argument.getClass().equals(Fields.class))
	 					return true;
	 				else return false;
	 			}
	 	    }
	        
	        
	        if(componentID == null)
	        	when(tuple.getSourceComponent()).thenThrow(new RuntimeException("No SourceComponent available for that MockTuple!"));
	        else
	        	when(tuple.getSourceComponent()).thenReturn(componentID);
	        
	        if(streamID == null)
	        	when(tuple.getSourceStreamId()).thenThrow(new RuntimeException("No SourceStreamID available for that MockTuple!"));
	        else
	        	when(tuple.getSourceStreamId()).thenReturn(streamID);
	        
	        
	        if(vals == null){
	        	when(tuple.getValues()).thenThrow(new RuntimeException("No Values available for that MockTuple!"));
	        	when(tuple.select(argThat(new IsAnyFieldsSelector()))).thenThrow(new RuntimeException("No Values available for that MockTuple!"));
	        }
        	else{
	        	when(tuple.getValues()).thenReturn(vals);
	        	when(tuple.select(argThat(new IsAnyFieldsSelector()))).thenReturn(vals);
        	}
	        
	        
	        if(keyConfig.equals(KeyConfigFactory.DefaultKey())){
		        when(tuple.getFields()).thenThrow(new RuntimeException("No Fields available for that MockTuple!"));
	        }
	        else if(keyConfig.equals(KeyConfigFactory.BySource())){
	        	when(tuple.getFields()).thenThrow(new RuntimeException("No Fields available for that MockTuple!"));
	        }
	        else if(keyConfig.equals(KeyConfigFactory.ByFields(keyFields))){
	        	when(tuple.getFields()).thenReturn(new Fields(keyFields));
	        }
	        
	        
	        
	        if((vals == null) && (componentID != null) && (streamID != null)){
	        	when(tuple.toString()).thenAnswer(new Answer<String>(){
		        	
		        	public String answer(InvocationOnMock invocation)
							throws Throwable {
		        		String toString = TupleMock.toComponentTupleString((Tuple) invocation.getMock());
						return toString;
					}
		        });
	        }
	        else if(vals != null){
		        when(tuple.toString()).thenAnswer(new Answer<String>(){
					public String answer(InvocationOnMock invocation)
							throws Throwable {
						String toString = TupleMock.toValueTupleString((Tuple) invocation.getMock());
						return toString;
					}
		        });
	        }

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
	    	tupleString += ")>";
	    	
	    	return tupleString;
	    }

}
