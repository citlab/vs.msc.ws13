package de.tu_berlin.citlab.testsuite.tests.filterTests;


import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.TestSetup;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger.LoD;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;
import de.tu_berlin.citlab.testsuite.tests.skeletons.OperatorTest;


public class FilterOperatorTest extends OperatorTest
{
	public static final String TAG = "OperatorTest";
	
	@Override
	protected void configureDebugLogger() 
	{
		DebugLogger.setEnabled(true);
		DebugLogger.setConsoleOutput(LoD.DEFAULT, true);
		DebugLogger.appendTimeToOutput(true);
		DebugLogger.appendCounterToOutput(true);
		
	}
	
	@Override
	protected List<Tuple> generateInputValues()
	{
		List<Tuple> inputTuples = new ArrayList<Tuple>(2);
		inputTuples.add(TupleMock.mockTuple(new Values(1,2,3)));
		inputTuples.add(TupleMock.mockTuple(new Values(4,5,6)));
		return inputTuples;
	}
	
	@Override
	protected IOperator initOperator(final List<Tuple> inputTuples) {
		Fields inputFields = new Fields("key", "value"); //TODO: should be declared somewhere else...
		
		FilterUDF filter= new FilterUDF(){
			private static final long	serialVersionUID	= 1L;
			private int count = 0;
			
			public Boolean evaluate(Tuple t)
			{
				//Test that inputTuples and Tuple t is the same for each iteration:
				if(t.equals(inputTuples.get(count))){
					count ++;
					DebugLogger.printAndLog_Message(LoD.DEFAULT, TAG, "Evaluation for FilterUDF returned true!", "Evaluated Tuple: "+ t.toString());
					return true;
				}
				else return false;
			}
		};
		
		FilterOperator testFilterOp = new FilterOperator(inputFields, filter);
		return testFilterOp;
	}
	
	@Override
	protected List<List<Object>> assertOutput(final List<Tuple> inputTuples)
	{
		List<List<Object>> outputVals = new ArrayList<List<Object>>();
		for(Tuple actTuple : inputTuples){
			outputVals.add(actTuple.getValues());
		}

		return outputVals;
	}
}
