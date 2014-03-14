package de.tu_berlin.citlab.storm.topologies.algotrading;

import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.topologies.algotrading.bolts.Filter;
import de.tu_berlin.citlab.storm.topologies.algotrading.bolts.FilterDropping;
import de.tu_berlin.citlab.storm.topologies.algotrading.bolts.SlopeCalculator;
import de.tu_berlin.citlab.storm.topologies.algotrading.bolts.StockSlaughterhouser;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.TimeWindow;

public class AlgorithmicTrading {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		// TODO: TBD
		builder.setSpout("spout", new BaseRichSpout() {

			private SpoutOutputCollector collector;
			private int offset = 0;
			
			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				declarer.declare(new Fields("share", "price", "time"));

			}

			public void open(Map conf, TopologyContext context,
					SpoutOutputCollector collector) {
				this.collector = collector;

			}

			public void nextTuple() {
				collector.emit(new Values("aktie1", (double) 1000 + offset++, System.currentTimeMillis()));
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, 1);

		builder.setBolt("slope_calculator",
			new UDFBolt(
				new Fields("share", "slope", "time"),
				new SlopeCalculator(),
				// we always need two tuples for slope calculation
				// slide to prevent "gaps"
				new CountWindow<Tuple>(2, 1),
				// group tuples by share into distinct windows
				KeyConfigFactory.ByFields("share")
			), // UDFBolt
			1  // parallelism hint
		). // builder.setBolt
		// make sure every worker is responsible for a distinct set of shares 
		fieldsGrouping("spout", new Fields("share"));
		
		builder.setBolt("filter_dropping",
		new UDFBolt(
				new Fields("share", "slope", "time"),
				new Filter("share") {
					protected boolean emitTuple(Tuple tuple) {
						return tuple.getDoubleByField("share") < 0;
					}
				}
				// no windowing or grouping, filter immediately  
			), // UDFBolt
			1  // parallelism hint
		). // builder.setBolt
		// make sure every worker is responsible for a distinct set of shares 
		fieldsGrouping("spout", new Fields("share"));
		
		builder.setBolt("filter_gaining",
		new UDFBolt(
				new Fields("share", "slope", "time"),
				new Filter("share") {
					protected boolean emitTuple(Tuple tuple) {
						return tuple.getDoubleByField("share") > 0;
					}
				}
				// no windowing or grouping, filter immediately  
			), // UDFBolt
			1  // parallelism hint
		). // builder.setBolt
		// make sure every worker is responsible for a distinct set of shares 
		fieldsGrouping("spout", new Fields("share"));
		
		
		builder.setBolt("stock_slaughterhouse",
			new UDFBolt(
				new Fields("share", "slope", "time"),
				new StockSlaughterhouser(),
				// gather share slopes for one second, then evaluate
				new TimeWindow<Tuple>(1000),
				// no distinct windows
				null,
				// if the slope calculator emitted for whatever reasons two tuples group them
				KeyConfigFactory.ByFields("share")
			), // UDFBolt
			1  // parallelism hint
		). // builder.setBolt
		// make sure every worker is responsible for a distinct set of shares 
		fieldsGrouping("share_slope", new Fields("share"));
		
		
		Config conf = new Config();
		conf.setDebug(true);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("algorithmic-trading", conf, builder.createTopology());
	}

}
