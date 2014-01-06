package de.tu_berlin.citlab.testsuite.tests;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.Context;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.helpers.TestSetup;
import de.tu_berlin.citlab.testsuite.tests.skeletons.OperatorTest;

public class MergeOperatorTest extends OperatorTest
{

	@Override
	protected List<Values> generateInputValues()
	{
		return TestSetup.generateValueList(new Values(1));
	}

	@Override
	protected Context initContext()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected IOperator initOperator()
	{
		IOperator mergeOperator = new IOperator(){
			private static final long	serialVersionUID	= 1L;

			public List<Values> execute(List<Values> param, Context context)
			{
				for(Values actVal : param){
					int one = (Integer) actVal.get(0);
					
				}
				return new ArrayList<Values>(new Values(one + 2));
			}
			
		}
		return null;
	}

	@Override
	protected List<Values> assertOutput(List<Values> inputValues)
	{
		// TODO Auto-generated method stub
		return null;
	}

}
