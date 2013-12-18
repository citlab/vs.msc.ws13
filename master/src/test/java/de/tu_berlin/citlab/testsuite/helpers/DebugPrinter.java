package de.tu_berlin.citlab.testsuite.helpers;


import java.util.List;

import de.tu_berlin.citlab.testsuite.mocks.MockTuple;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public final class DebugPrinter
{

	public static String toString(List<Values> valList)
	{
		String output = "List<Values>(";
		for(int n = 0 ; n < valList.size() ; n++){
			Values actVals = valList.get(n);
			output += "[";
			
			for(int i = 0 ; i < actVals.size() ; i++){
				Object actObj = actVals.get(i);
				output += actObj.toString();
				
				if(i+1 != actVals.size())
	    			output += ", ";
				else
					output +="]";
			}
			
			if(n+1 != valList.size())
    			output += ", ";
		}
		output += ")";
		
		return output;
	}
	
	
	public static String toString(Tuple tuple)
	{
		return MockTuple.toString(tuple);
	}
}
