package de.tu_berlin.citlab.testsuite.tests;


import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.udf.Context;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.helpers.TestSetup;
import de.tu_berlin.citlab.testsuite.tests.skeletons.OperatorTest;


public class FilterOperatorTest extends OperatorTest
{

	@Override
	protected List<Values> generateInputValues()
	{
		List<Values> inputVals = TestSetup.generateValueList(new Values(1,2,3), new Values(4,5,6), new Values(7,8,9));
		return inputVals;
	}

	@Override
	protected IOperator initOperator()
	{
		FilterOperator testFilterOp = new FilterOperator(new FilterUDF()
		{
			private static final long	serialVersionUID	= 1L;

			public Boolean execute(Values param, Context context)
			{
				if(param.contains(1))
					return true;
				else return false;
			}			
		});
		return testFilterOp;
	}

	@Override
	protected Context initContext()
	{
		return null;
	}

	@Override
	protected List<Values> assertOutput(List<Values> inputVals)
	{
		List<Values> outputVals = new ArrayList<Values>(1);
		outputVals.add(inputVals.get(0));
		return outputVals;
	}


}
