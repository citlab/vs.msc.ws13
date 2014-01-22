package de.tu_berlin.citlab.testsuite.mocks;


import static org.mockito.Mockito.*;

import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger.LoD;
import de.tu_berlin.citlab.testsuite.helpers.DebugPrinter;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;


public final class OutputCollectorMock
{	
		public final static String TAG = "OutputCollectorMock";
		public static List<List<Object>> output;

        public static void resetOutput()
        {
            if(output != null)
                output.clear();
        }

		
	    public static OutputCollector mockOutputCollector() 
	    {
	    	OutputCollector oColl = mock(OutputCollector.class);
	    	output = new ArrayList<List<Object>>();
	    	
	    	class IsAnyObjectList extends ArgumentMatcher<List<Object>>
	 	    {
	 			@Override
	 			public boolean matches(Object argument)
	 			{
	 				List<Object> objList = (List) argument;
	 				if(argument.getClass().equals(objList.getClass())){
	 					return true;
	 				}
	 				else return false;
	 			}
	 	    	
	 	    }
	    	
	    	class IsAnyTuple extends ArgumentMatcher<Tuple>
		    {
				@Override
				public boolean matches(Object argument)
				{
					if(argument.getClass().equals(Tuple.class))
						return true;
					else return false;
				}
		    	
		    }
	        
	    	when(oColl.emit(argThat(new IsAnyObjectList()))).thenAnswer(new Answer<List<Object>>(){
	    		public List<Object> answer(InvocationOnMock invocation)
	    				throws Throwable {
	    			
					//the objectList which was emitted by the Output-Coll Mock:
					List<Object> emissionList = (List<Object>) invocation.getArguments()[0];
					output.add(emissionList);
                    DebugLogger.printAndLog_Message(LoD.DETAILED, TAG, "Outputcollector emitted: ", DebugPrinter.toObjectListString(emissionList));
					return emissionList;
				}
	    	});
	    	
	        doNothing().when(oColl).ack(argThat(new IsAnyTuple()));
	        
	        return oColl;
	    }

}
