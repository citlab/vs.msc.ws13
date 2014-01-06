package de.tu_berlin.citlab.testsuite.tests.skeletons;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IKeyConfig;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.testsuite.mocks.UDFBoltMock;


abstract public class UDFBoltTest
{
	protected class UDFFields
	{
		public final Fields inputFields;
		public final Fields outputFields;
		public final Fields keyFields;
		
		public UDFFields(Fields inputFields, Fields outputFields)
		{
			this(inputFields, outputFields, null);
		}
		
		public UDFFields(Fields inputFields, Fields outputFields, Fields keyFields)
		{
			this.inputFields = inputFields;
			this.outputFields = outputFields;
			this.keyFields = keyFields;
		}
	}
	
	
	private UDFBoltMock udfBolt;
	
	private List<Tuple> inputTuples;
	private UDFFields udfFields;
	private IOperator operator;
	private Window<Tuple, List<Tuple>> window;
	private IKeyConfig keyConfig;
	
	
	@Before	
	public void initTestSetup()
	{
		inputTuples = this.generateInputTuples();
		udfFields = this.initUDFFields();
		operator = this.initOperator();
		window = this.initWindow();
		keyConfig = this.initKeyConfig();
		
		if (window == null)
			udfBolt = new UDFBoltMock(udfFields.inputFields, udfFields.outputFields, operator);
		else
			udfBolt = new UDFBoltMock(udfFields.inputFields, udfFields.outputFields, operator, window, udfFields.keyFields, keyConfig);
	}
	
	
	@Test
	public void testUDFBolt()
	{
		System.out.println("================================================");
		System.out.println("=========== Starting UDFBolt Test... =========== \n");
		
		
		long startTime = System.currentTimeMillis();
		
		for(Tuple actTuple : inputTuples){
			udfBolt.execute(actTuple);
		}
		
		long endTime = System.currentTimeMillis();
		long inputTimeDiff = endTime - startTime;
		
		//TODO: assertTrue on OutputCollectorMock here.
		
		
		System.out.println("\nSummary:");
		System.out.println("\t Number of Input-Tuples: "+ inputTuples.size());
		System.out.println("\t Number of Tick-Tuples: "); //TODO: implement
		System.out.println("\t Time to execute input:"+ inputTimeDiff +" ms.");
		
		System.out.println("\n=========== Finished UDFBolt Test! ===========");
		System.out.println("==============================================");
	}
	
	
	@After
	public void exitTestSetup()
	{
		udfBolt = null;
		
		udfFields = null;
		operator = null;
		window = null;
		keyConfig = null;
	}
	
	
	abstract protected List<Tuple> generateInputTuples();
	
	abstract protected UDFFields initUDFFields();
	abstract protected IOperator initOperator();
	abstract protected Window<Tuple, List<Tuple>> initWindow();
	abstract protected IKeyConfig initKeyConfig();
	
	abstract protected List<Object> assertOutput(List<Tuple> inputTuples);
	
}
