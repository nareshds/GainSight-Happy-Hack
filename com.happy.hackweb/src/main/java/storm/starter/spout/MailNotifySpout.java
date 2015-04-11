package storm.starter.spout;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.codehaus.jettison.json.JSONObject;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class MailNotifySpout implements IRichSpout{
	private SpoutOutputCollector collector;
	private TopologyContext context;
	public static BlockingQueue<JSONObject> queue = new LinkedBlockingQueue<JSONObject>();

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;
		//queue = (LinkedBlockingQueue<String>)conf.get("queue"); 
	}

	public void nextTuple() {
		try{
		JSONObject ret = queue.take();
		if (ret == null) {
			//Utils.sleep(50);
		} else {
			System.out.println("Polled data "+ret);
			collector.emit(new Values(ret), ret);
		}
		}catch(Exception ex){
			ex.printStackTrace();
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
