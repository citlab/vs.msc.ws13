package de.tu_berlin.citlab.testsuite.tests.filterTests;


import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.helpers.TestSetup;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;
import de.tu_berlin.citlab.testsuite.tests.skeletons.OperatorTest;


public class FilterOperatorTest extends OperatorTest
{
	@Override
	protected void configureDebugLogger() 
	{
		//TODO: implement.
		
	}
	
	@Override
	protected List<Tuple> generateInputValues()
	{
		List<Tuple> inputTuples = new ArrayList<Tuple>(2);
		inputTuples.add(TupleMock.mockTuple(new Values(1,2,3)));
		inputTuples.add(TupleMock.mockTickTuple());
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
					System.out.println("Evaluation successful!");
					return true;
				}
				else return false;
			}
		};
		
		FilterOperator testFilterOp = new FilterOperator(inputFields, filter);
		return testFilterOp;
	}
	
	@Override
	protected List<Object> assertOutput(final List<Tuple> inputTuples)
	{
		List<Object> outputVals = new ArrayList<Object>(1);
		outputVals.add(inputTuples.get(0));//TODO: check.
		return outputVals;
	}
}
