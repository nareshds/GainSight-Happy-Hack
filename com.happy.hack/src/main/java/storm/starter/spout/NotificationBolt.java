package storm.starter.spout;

import java.util.Map;
import java.util.WeakHashMap;

import org.json.simple.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class NotificationBolt implements IRichBolt {
	private OutputCollector collector;
	private WeakHashMap<String, String> inAppNotify;

	public void cleanup() {
		for (Map.Entry<String, String> entry : inAppNotify.entrySet()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
	}

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		JSONObject object = (JSONObject) tuple;
		String userID = object.get("User ID").toString();
		String event = object.get("Event").toString();
		String event_type = object.get("Event Type").toString();
		if(inAppNotify == null) inAppNotify = new WeakHashMap<String, String>();
		if ("Mobile".equals(event_type)) {
			inAppNotify.put(userID, event);
		}
		collector.ack(tuple);
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word"));

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
