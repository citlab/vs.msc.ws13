package de.tu_berlin.citlab.testsuite.tests.skeletons;


import java.util.List;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.Context;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.helpers.DebugPrinter;


abstract public class OperatorTest
{
	private List<Values> inputValues;
	private IOperator operator;
	private Context context;
	
	
	@Before	
	public void initTestSetup()
	{
		inputValues = this.generateInputValues();
		operator = this.initOperator();
	}
	
	
	@Test
	public void testOperator()
	{
		List<Values> result = operator.execute(inputValues, context);
		List<Values> assertRes = assertOutput(inputValues);
		assertTrue("Operator.execute(..) result is not equal to asserted Output! \n"+
				   "Operator Result: "+ DebugPrinter.toString(result) +"\n"+
				   "Asserted Output: "+ DebugPrinter.toString(assertRes), result.equals(assertRes));
	}
	
	
	@After
	public void exitTestSetup()
	{
		inputValues = null;
		operator = null;
		context = null;
	}
	
	
	
	abstract protected List<Values> generateInputValues();
	abstract protected Context initContext();
	abstract protected IOperator initOperator();
	
	abstract protected List<Values> assertOutput(List<Values> inputValues);
}
