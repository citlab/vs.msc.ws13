package de.tu_berlin.citlab.storm.tick;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TickVerifierBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -7115214742789919480L;

	private long lastTick = -1;

	private long constructTime;

	private long prepareTime;

	private int tickInterval;

	private static Map<Integer, List<Long>> stats = new HashMap<Integer, List<Long>>();

	private static void stat() {
		Object[] keys = stats.keySet().toArray();
		List<Float> avgs = new ArrayList<Float>();
		for (Integer key : stats.keySet()) {
			float count = 0;
			float sum = 0;
			for (Long val : stats.get(key)) {
				count++;
				sum += (float) val;
			}
			avgs.add(sum / count);
		}

		System.out
				.printf("interval: "
						+ StringUtils.repeat("%4ds  ", keys.length) + "\n",
						keys);
		System.out.printf(
				"  jitter: " + StringUtils.repeat("%4.2fms ", keys.length)
						+ "\n", avgs.toArray());
	}

	public TickVerifierBolt(int tickInterval) {
		this.tickInterval = tickInterval;
		constructTime = System.currentTimeMillis();
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context) {
		prepareTime = System.currentTimeMillis();
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		if (TupleHelper.isTickTuple(input)) {
			long now = System.currentTimeMillis();
			if (lastTick > 0) {
				long diff = now - lastTick - (tickInterval * 1000);
				lastTick = now;
				if (!stats.containsKey(tickInterval)) {
					stats.put(tickInterval, new ArrayList<Long>());
				}
				List<Long> curStat = stats.get(tickInterval);
				curStat.add(diff);
				if (tickInterval == 1) {
					stat();
				}
			} else {
				System.out.println("First Tick Tuple [" + tickInterval + "s]");
				System.out.println("Time since construct: "
						+ (now - constructTime) + "ms");
				System.out.println("Time since prepare: " + (now - prepareTime)
						+ "ms");
				lastTick = now;
			}
		}
	}

	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickInterval);
		return conf;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
