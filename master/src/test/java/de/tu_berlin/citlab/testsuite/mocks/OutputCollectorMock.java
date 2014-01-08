package de.tu_berlin.citlab.testsuite.mocks;


import static org.mockito.Mockito.*;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;


public final class OutputCollectorMock
{	
		public static List<List<Object>> output;
		
		
	    public static OutputCollector mockOutputCollector() 
	    {
	    	OutputCollector oColl = mock(OutputCollector.class);
	    	output = new ArrayList<List<Object>>();
	    	
	    	class IsAnyObjectList extends ArgumentMatcher<List<Object>>
	 	    {
	 			@Override
	 			public boolean matches(Object argument)
	 			{
	 				System.out.println("Matching?");
	 				List<Object> objList = (List) argument;
	 				if(argument.getClass().equals(objList.getClass())){
	 					System.out.println("Matches!");
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
					System.out.println("Emitted "+ emissionList.size() +" values.");
					return emissionList;
				}
	    	});
	    	
	        doNothing().when(oColl).ack(argThat(new IsAnyTuple()));
	        
	        return oColl;
	    }

}
