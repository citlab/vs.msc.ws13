package de.tu_berlin.citlab.storm.topologies.algotrading.bolts;

import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;

@SuppressWarnings("serial")
public abstract class Filter implements IOperator {

	private Logger log = Logger.getLogger(getClass());
	
	private String field = null;
	
	public Filter(String field) {
		this.field = field;
	}
	
	protected abstract boolean emitTuple(Tuple tuple);
	
	public void execute(List<Tuple> input, OutputCollector collector) {
		if(field == null) {
			log.error("field is null");
		}
		else {
			// don't dictate any windowing behavior 
			for(Tuple tuple : input) {
				// try-catch for the case of a wrong field name
				try {
					if(emitTuple(tuple)) {
						collector.emit(tuple, tuple.getValues());
					}
				}
				catch(IllegalArgumentException e) {
					log.error(String.format("field '%s' is not in tuple: '%s'", field, tuple.getFields()));
				}
			}
		}
	}

}
