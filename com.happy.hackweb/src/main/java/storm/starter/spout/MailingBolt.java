package storm.starter.spout;

import java.util.Map;

import org.codehaus.jettison.json.JSONObject;


import com.mongodb.DBObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class MailingBolt implements IRichBolt{

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple input) {
		JSONObject object = (JSONObject) input.getValues().get(0);
		
		try {
			String userId = object.get("userid").toString();
			DBObject userdbObj = new MongoDBQuery().getUser(userId);
			new MongoDBQuery().setEvent(object);
			DBObject mailObject = new MongoDBQuery().getEvent(userId);
			System.out.println("========Success========");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
