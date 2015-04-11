package storm.starter.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class InAppNotificationSpout implements IRichSpout {
	private SpoutOutputCollector collector;
	private TopologyContext context;
	LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000);;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;
	}

	public void nextTuple() {

		String ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(ret), ret);
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	public void close() {
	}

	public boolean isDistributed() {
		return false;
	}

	public void activate() {
	}

	public void deactivate() {
	}

	public void ack(Object msgId) {
	}

	public void fail(Object msgId) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}