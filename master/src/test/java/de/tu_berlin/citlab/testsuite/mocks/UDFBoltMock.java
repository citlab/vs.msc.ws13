package de.tu_berlin.citlab.testsuite.mocks;

import java.util.List;
import java.util.Map;

import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.Window;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.DebugPrinter;


public class UDFBoltMock extends UDFBolt
{
    public static final String TAG ="UDFBoltMock";
	private static final long serialVersionUID = 1L;


    public final OutputCollector getOutputCollectorMock() { return this.collector; };

/* Constructors: */
/* ============= */
	
	public UDFBoltMock(Fields outputFields, IOperator operator) 
	{
		this(outputFields, operator, new CountWindow<Tuple>(1));
	}

	public UDFBoltMock(Fields outputFields, IOperator operator,
			Window<Tuple, List<Tuple>> window) 
	{
		this(outputFields, operator, window, KeyConfigFactory.DefaultKey());
	}

	public UDFBoltMock(Fields outputFields, IOperator operator,
			Window<Tuple, List<Tuple>> window, IKeyConfig keyConfig) 
	{
		super(outputFields, operator, window, keyConfig);
		this.prepare(null, null, null); //TODO: maybe adjust this?
	}
	
	
/* Public Methods: */
/* =============== */
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) 
	{
		super.prepare(stormConf, context, collector);
		this.collector = OutputCollectorMock.mockOutputCollector();
	}

    @Override
    public void execute(Tuple input) {
        super.execute(input);

        DebugLogger.printAndLog_Message(DebugLogger.LoD.DETAILED, TAG, "Executed Tuple.",
                "Tuple Values: "+ DebugPrinter.toObjectListString(input.getValues()));
    }

    @Override
    protected void executeBatches(List<List<Tuple>> windows) {
        super.executeBatches(windows);

        //TODO: write List<List<Tuple>> as an DebugPrinter String and log it here.
        DebugLogger.printAndLog_Message(DebugLogger.LoD.DETAILED, TAG, "Executed Batch of Tuple Window.");
    }
}
