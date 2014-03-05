package de.tu_berlin.citlab.storm.tests.filterTests;


import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.OperatorTestMethods;


public class FilterOperatorTest extends OperatorTest implements OperatorTestMethods
{
	public static final String TAG = "OperatorTest";


    public FilterOperatorTest(Fields inputFields) {
        super("FilterOperator", inputFields);
    }


    @Override
	public List<Tuple> generateInputTuples()
	{
		List<Tuple> inputTuples = new ArrayList<Tuple>(2);
		inputTuples.add(TupleMock.mockTuple(new Values(1,2,3)));
		inputTuples.add(TupleMock.mockTuple(new Values(4,5,6)));
		return inputTuples;
	}
	
	@Override
	public IOperator initOperator(final List<Tuple> inputTuples) {

		FilterUDF filter= new FilterUDF(){
			private static final long	serialVersionUID	= 1L;
			private int count = 0;
			
			public Boolean evaluate(Tuple t)
			{
				//Test that inputTuples and Tuple t is the same for each iteration:
				if(t.equals(inputTuples.get(count))){
					count ++;
                    //TODO: use LOGGER here:
//					DebugLogger.printAndLog_Message(LoD.DEFAULT, TAG, "Evaluation for FilterUDF returned true!", "Evaluated Tuple: "+ t.toValString());
					return true;
				}
				else return false;
			}
		};
		
		FilterOperator testFilterOp = new FilterOperator(this.getInputFields(), filter);
		return testFilterOp;
	}
	
	@Override
	public List<List<Object>> assertOutput(final List<Tuple> inputTuples)
	{
        List<List<Object>> outputVals = new ArrayList<List<Object>>();
        outputVals.add(new Values(1,2,3));
        outputVals.add(new Values(4,5,6));

		return outputVals;
	}
}
