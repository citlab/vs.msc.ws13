package de.tu_berlin.citlab.testsuite.mocks;

import java.util.List;
import java.util.Map;

import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.udf.IKeyConfig;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.Window;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


public class UDFBoltMock extends UDFBolt
{
	private static final long serialVersionUID = 1L;


/* Constructors: */
/* ============= */
	
	public UDFBoltMock(Fields inputFields, Fields outputFields, IOperator operator) 
	{
		this(inputFields, outputFields, operator, new CountWindow<Tuple>(1));
	}

	public UDFBoltMock(Fields inputFields, Fields outputFields, IOperator operator,
			Window<Tuple, List<Tuple>> window) 
	{
		this(inputFields, outputFields, operator, window, null, null);
	}

	public UDFBoltMock(Fields inputFields, Fields outputFields, IOperator operator,
			Window<Tuple, List<Tuple>> window, Fields keyFields) 
	{
		this(inputFields, outputFields, operator, window, keyFields, null);
	}
	
	public UDFBoltMock(Fields inputFields, Fields outputFields, IOperator operator,
			Window<Tuple, List<Tuple>> window, Fields keyFields, IKeyConfig keyConfig) 
	{
		super(inputFields, outputFields, operator, window, keyFields, keyConfig);
	}
	
	
/* Public Methods: */
/* =============== */
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
		this.collector = MockOutputCollector.mockOutputCollector();
	}


}
