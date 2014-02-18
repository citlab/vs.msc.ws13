package de.tu_berlin.citlab.storm.topologies;

import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

public class AlgorithmicTrading {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		// TODO: TBD
		builder.setSpout("spout", new BaseRichSpout() {

			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				declarer.declare(new Fields("share", "price", "time"));

			}

			public void open(Map conf, TopologyContext context,
					SpoutOutputCollector collector) {
				// TODO Auto-generated method stub

			}

			public void nextTuple() {
				// TODO Auto-generated method stub

			}
		}, 1);

		builder.setBolt("share_slope",
			new UDFBolt(
				new Fields("share", "slope", "time"),
				new IOperator() {
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
				},
				// we always need two tuples for slope calculation
				// slide to prevent "gaps"
				new CountWindow<Tuple>(2, 1),
				// group tuples by share into distinct windows
				KeyConfigFactory.ByFields("share")
			), 1).
			// make sure every worker is responsible for a distinct set of shares 
			fieldsGrouping("spout", new Fields("share")
		);

		Config conf = new Config();
		conf.setDebug(true);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("algorithmic-trading", conf, builder.createTopology());
	}

}
