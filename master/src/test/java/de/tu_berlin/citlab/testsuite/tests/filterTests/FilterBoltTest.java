package de.tu_berlin.citlab.testsuite.tests.filterTests;


import java.util.List;

import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;

import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.testsuite.testSkeletons.UDFBoltTest;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.UDFBoltTestMethods;


//TODO: implement class with new Test-Suite
public class FilterBoltTest extends UDFBoltTest implements UDFBoltTestMethods
{

/* Global Test Params: */
/* =================== */
	
	/*private static int keyIDCount = 5;
	private static int keyListCount = 10;
	private static int maxValueCount = 10;
	private static int iterations = 10000;
	private static int tickInterval = 10;
	
	private static Fields inputFields = new Fields("key", "value");
	private static Fields outputFields = new Fields("key", "value");
	private static Fields keyFields = new Fields("key1");*/

    public FilterBoltTest(OperatorTest opTest, Fields outputFields) {
        super("FilterOperator", opTest, outputFields);
    }

//    @Override
//	public List<Tuple> generateInputTuples()
//	{
//        List<Tuple> inputTuples = this.getOperatorInputTuples();
//        inputTuples.add(TupleMock.mockTickTuple());
//        return inputTuples;
//	}

	@Override
	public Window<Tuple, List<Tuple>> initWindow()
	{
		return null;
	}

	@Override
	public IKeyConfig initKeyConfig()
	{
		return null;
	}

	/*@Override
	protected List<Object> assertOutput(List<Tuple> inputTuples)
	{
		// TODO Auto-generated method stub
		return null;
	}*/
	
	

}
