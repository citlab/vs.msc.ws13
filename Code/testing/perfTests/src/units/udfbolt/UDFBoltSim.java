package units.udfbolt;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class UDFBoltSim
{
	protected final Fields inputFields;
	protected WindowHandler windowHandler;

	/*
	public UDFBolt(Fields inputFields, Fields outputFields, IOperator operator) {
		this(inputFields, outputFields, operator, new CountWindow<Tuple>(1));
	}

	public UDFBolt(Fields inputFields, Fields outputFields, IOperator operator,
			Window<Tuple, List<Tuple>> window) {
		this(inputFields, outputFields, operator, window, null);
	}*/

	public UDFBoltSim(Fields inputFields, Window<Tuple, List<Tuple>> window, Fields keyFields) {
		this.inputFields = inputFields;
		windowHandler = new WindowHandler(window, keyFields);
	}


	public void execute(Tuple input) {
			windowHandler.add(input);
			if (windowHandler.isSatisfied()) {
				executeBatches(windowHandler.flush());
			}
	}

	private void executeBatches(List<List<Tuple>> windows) {
		for (List<Tuple> window : windows) {
			List<List<Object>> inputValues = new ArrayList<List<Object>>();
			for (Tuple tuple : window) {
				inputValues.add(tuple.select(inputFields));
			}
			List<List<Object>> outputValues = execute(inputValues);
		}
	}


	private List<List<Object>> execute(List<List<Object>> inputValues) 
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
