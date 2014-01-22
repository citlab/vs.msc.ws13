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

		public final static String TAG = "TupleMock";


        enum IKeyType {UNDEFINED, DEFAULT, BY_FIELDS, BY_SOURCE}



	    public static Tuple mockTickTuple(String... logTags)
	    {
            TupleLogger.logTupleMessage("Created Tick-Tuple Mock.",
                           new String[]{"SYS_COMP_ID: "+ Constants.SYSTEM_COMPONENT_ID,
                                        "SYS_TICK-STREAM_ID: "+ Constants.SYSTEM_TICK_STREAM_ID},
                                        logTags);
	        return TupleMock.mockSysIDTuple(Constants.SYSTEM_COMPONENT_ID, Constants.SYSTEM_TICK_STREAM_ID);
	    }
	
	    public static Tuple mockSysIDTuple(String componentID, String streamID, String... logTags)
	    {
	    	TupleLogger.logTupleMessage("Created Source-Tuple Mock.",
                           new String[]{"SYS_COMP_ID: "+ componentID,
                                        "SYS_TICK-STREAM_ID: "+ streamID},
                                        logTags);
	        return TupleMock.mockTuple(componentID, streamID, null, KeyConfigFactory.DefaultKey(), IKeyType.UNDEFINED);
	    }
	    
	    
	    public static Tuple mockTuple(Values vals, String... logTags)
	    {
	    	TupleLogger.logTupleMessage("Created Tuple Mock by DefaultKey.",
                           new String[]{"Vals: "+ DebugPrinter.toString(vals),
					                    "IKeyConfig: "+ "DEFAULT-Key"},
                                        logTags);
	    	return TupleMock.mockTuple(null, null, vals, KeyConfigFactory.DefaultKey(), IKeyType.DEFAULT);
	    }
	    
	    public static Tuple mockTupleBySource(String componentID, Values vals, String... logTags)
	    {
	    	TupleLogger.logTupleMessage("Created Tuple Mock by Source.",
                           new String[]{"Vals: "+ DebugPrinter.toString(vals),
					                    "SYS_COMP_ID: "+ componentID},
                                        logTags);
	    	return TupleMock.mockTuple(componentID, null, vals, KeyConfigFactory.BySource(), IKeyType.BY_SOURCE);
	    }
	    
	    public static Tuple mockTupleByFields(Values vals, Fields fields, String... logTags)
	    {
	    	TupleLogger.logTupleMessage("Created Tuple Mock by Fields.",
                           new String[]{"Vals: "+ DebugPrinter.toString(vals),
					                    "Fields: "+ DebugPrinter.toString(fields)},
                                        logTags);

            String[] fieldsStr = new String[fields.size()];
            for(int n = 0 ; n < fields.size() ; n++){
                fieldsStr[n] = fields.get(n);
            }
	    	return TupleMock.mockTuple(null, null, vals, KeyConfigFactory.ByFields(fields), IKeyType.BY_FIELDS, fieldsStr);
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

	    	return TupleMock.mockTuple(componentID, streamID, vals, keyConfig, IKeyType.UNDEFINED, keyFields);
	    }
	    

	    
	    private static Tuple mockTuple(final String componentID, final String streamID, final Values vals, final IKeyConfig keyConfig, final IKeyType keyType, final String... keyFields)
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

            class IsAnyString extends ArgumentMatcher<String>
            {
                @Override
                public boolean matches(Object argument){
                    if(argument.getClass().equals(String.class))
                        return true;
                    else return false;
                }
            }
	        
	        
	        if(componentID == null){
	        	when(tuple.getSourceComponent()).thenReturn("__null");
	        }
	        else
	        	when(tuple.getSourceComponent()).thenReturn(componentID);
	        
	        if(streamID == null){
	        	when(tuple.getSourceStreamId()).thenReturn("__null");
	        }
	        else
	        	when(tuple.getSourceStreamId()).thenReturn(streamID);
	        
	        
	        if(vals == null){
	        	
	        	when(tuple.getValues()).thenThrow(new RuntimeException("Trying to call getValues() on a MockTuple, that has no values!"));
	        	when(tuple.select(argThat(new IsAnyFieldsSelector()))).thenThrow(new RuntimeException("Trying to call select() on a MockTuple, that has no values!"));
	        }
        	else{
	        	when(tuple.getValues()).thenReturn(vals);
	        	when(tuple.select(argThat(new IsAnyFieldsSelector()))).thenReturn(vals);
        	}


	        if(keyType.equals(IKeyType.BY_FIELDS)){
	        	when(tuple.getFields()).thenReturn(new Fields(keyFields));
                when(tuple.getValueByField(argThat(new IsAnyString()))).thenAnswer(new Answer<Object>() {

                    public Object answer(InvocationOnMock invocation)
                            throws Throwable {
                        String keyField = (String) invocation.getArguments()[0];
                        //TODO: change to TupleLogger here:
                        DebugLogger.log_Message(LoD.DETAILED, TAG, "Searching for Tuple Value by Field.",
                                "Key-Field: " + keyField);

                        for (int n = 0; n < keyFields.length; n++) {
                            String actField = keyFields[n];
                            if (keyField.equals(actField)) {

                                DebugLogger.log_Message(LoD.DETAILED, TAG, "Found Tuple Value by Field.",
                                        "Value: " + vals.get(n).toString(),
                                        "Key-Field: " + actField);
                                return vals.get(n);
                            }

                        }
                        return null;
                    }
                });
	        }
            else{
                when(tuple.getFields()).thenThrow(new RuntimeException("Trying to call getFields() on a MockTuple that has no fields!"));
                when(tuple.getValueByField(argThat(new IsAnyString()))).thenThrow(new RuntimeException("Trying to call getValueByField(..) on a MockTuple that has no fields!"));
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
	    		String actVal = mockTuple.getValues().get(n).toString();
	    		tupleString += actVal;
	    		if(n+1 != valSize)
	    			tupleString += ", ";
	    	}
	    	tupleString += ")>";
	    	
	    	return tupleString;
	    }

}

final class TupleLogger
{
    public static void logTupleMessage(String logMsg, String[] logInfo, String... logTags)
    {
        //If no logTags are provided, use the static final TAG inside the TupleLogger.logTupleMessage():
        if(logTags.length == 0){
            if(logInfo.length == 0)
                DebugLogger.log_Message(LoD.DETAILED, TupleMock.TAG, logMsg);
            else
                DebugLogger.log_Message(LoD.DETAILED, TupleMock.TAG, logMsg, logInfo);
        }
        //If only one logTag is provided, use this inside the TupleLogger.logTupleMessage():
        else if(logTags.length == 1){
            if(logInfo.length == 0)
                DebugLogger.log_Message(LoD.DETAILED, logTags[0], logMsg);
            else
                DebugLogger.log_Message(LoD.DETAILED, logTags[0], logMsg, logInfo);
        }
        //If several logTags are provided, use these inside the DebugLogger.multiLog_Message():
        else{
            if(logInfo.length == 0)
                DebugLogger.multiLog_Message(LoD.DETAILED, logTags, logMsg);
            else
                DebugLogger.multiLog_Message(LoD.DETAILED, logTags, logMsg, logInfo);

        }
    }

    public static void logTupleError(String logMsg, String[] logInfo, String... logTags)
    {
        //If no logTags are provided, use the static final TAG inside the TupleLogger.logTupleMessage():
        if(logTags.length == 0){
            if(logInfo.length == 0)
                DebugLogger.log_Error(TupleMock.TAG, logMsg);
            else
                DebugLogger.log_Error(TupleMock.TAG, logMsg, logInfo);
        }
        //If only one logTag is provided, use this inside the TupleLogger.logTupleMessage():
        else if(logTags.length == 1){
            if(logInfo.length == 0)
                DebugLogger.log_Error(logTags[0], logMsg);
            else
                DebugLogger.log_Error(logTags[0], logMsg, logInfo);
        }
        //If several logTags are provided, use these inside the DebugLogger.multiLog_Message():
        else{
            if(logInfo.length == 0)
                DebugLogger.multiLog_Error(logTags, logMsg);
            else
                DebugLogger.multiLog_Error(logTags, logMsg, logInfo);

        }
    }
}