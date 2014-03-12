package de.tu_berlin.citlab.storm.topologies.algotrading.bolts;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.IOperator;

@SuppressWarnings("serial")
public class SlopeCalculator implements IOperator {

	public void execute(List<Tuple> param, OutputCollector collector) {
		// *** Validating input data ***
		// window size has to be two
		if (param.size() != 2) {
			throw new IllegalArgumentException("share_slope executer expected list with size of 2, but got list with size of " + param.size() + ": "+ param);
		}
		// window has to be correctly grouped
		else if(!param.get(0).getStringByField("share").equals(param.get(1).getStringByField("share"))) {
			throw new IllegalArgumentException("share_slope executer expected two tuples of the same share but the first got '" + param.get(0).getStringByField("share") + "' and the second '" + param.get(1).getStringByField("share") + "': "+ param);
		}
		// window has to be correctly sorted
		else if(param.get(0).getLongByField("time") >= param.get(1).getLongByField("time")) {
			throw new IllegalArgumentException("share_slope executer expected two tuples ascending sorted by time, but the first has '" + param.get(0).getLongByField("time") + "' and the second '" + param.get(1).getLongByField("time") + "': "+ param);
		}
		// *** Actual code, if everything is as expected ***
		else {
			// calculate slope
			double first = param.get(0).getDoubleByField("price");
			double last = param.get(1).getDoubleByField("price");
			double slope = (last - first) / first;
			// construct output
			String share = param.get(0).getStringByField("share");
			Values output = new Values(share, slope, System.currentTimeMillis());
			// emit with chaining
			collector.emit(param, output);
		}
	}
	
}