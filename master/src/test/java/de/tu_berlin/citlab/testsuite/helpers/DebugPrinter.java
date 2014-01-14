package de.tu_berlin.citlab.testsuite.helpers;


import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public final class DebugPrinter
{

/* List-Printing Methods: */
/* ====================== */
	
	public static String toValListString(List<Values> valList)
	{
		String output = "List<Values>(";
		for(int n = 0 ; n < valList.size() ; n++){
			Values actVals = valList.get(n);
			output += DebugPrinter.toString(actVals);
			
			if(n+1 != valList.size())
    			output += ", ";
		}
		output += ")";
		
		return output;
	}
	
	public static String toTupleListString(List<Tuple> tupleList)
	{
		String output = "List<Tuple>(";
		
		for(int n = 0 ; n < tupleList.size() ; n++){
			Tuple actTuple = tupleList.get(n);
			output += actTuple.toString(); //Assumed to use TupleMock, so toString() is valid here.
			
			if(n+1 != tupleList.size())
    			output += ", ";
		}
		output += ")";
		
		return output;
	}
	
	public static String toObjectListString(List<Object> objList)
	{
		String output = "List<Object>(";
		
		for(int n = 0 ; n < objList.size() ; n++){
			Object actObj = objList.get(n);
			output += actObj.toString();
			
			if(n+1 != objList.size())
    			output += ", ";
		}
		output += ")";
		
		return output;
	}
	
	
	public static String toObjectWindowString(List<List<Object>> objWindow)
	{
		String output = "List<List<Object>>((";
		for(int n = 0 ; n < objWindow.size(); n++){
			List<Object> actList = objWindow.get(n);
			output += DebugPrinter.toObjectListString(actList);
			
			if(n+1 != objWindow.size())
    			output += ", ";
		}
		output += ")";
		
		return output;
	}
	
	
	
/* Storm Element-Printing Methods: */
/* =============================== */
	
	public static String toString(Values vals)
	{
		String output ="[";
		for(int i = 0 ; i < vals.size() ; i++){
			Object actObj = vals.get(i);
			output += actObj.toString();
			
			if(i+1 != vals.size())
    			output += ", ";
			else
				output +="]";
		}
		
		return output;
	}
	
	
	public static String toString(Fields fields)
	{
		String output ="[";
		
		for(int i = 0 ; i < fields.size() ; i++){
			String actField = fields.get(i);
			output += actField;
			
			if(i+1 != fields.size())
    			output += ", ";
			else
				output +="]";
		}
		
		return output;
	}
}
