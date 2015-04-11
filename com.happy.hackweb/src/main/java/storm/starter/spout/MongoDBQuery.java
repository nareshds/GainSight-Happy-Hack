package storm.starter.spout;

import java.net.UnknownHostException;

import org.json.simple.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class MongoDBQuery {
	private static MongoClient mongoClient = null;
	private static DB db =null;
	static{
		try {
			mongoClient = new MongoClient("127.0.0.1", 27017);
			db = mongoClient.getDB("gainsight");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public DBObject getUser(String userId){
		DBCollection coll = db.getCollection("users_master");
		
		BasicDBObject fields = new BasicDBObject();
		fields.put("id", userId);
		DBCursor dbCursor = coll.find(fields);
		DBObject dbObject = null;
		while(dbCursor.hasNext()){
			dbObject = dbCursor.next();
			System.out.println(dbObject.get("email"));
			break;
		}
		
		return dbObject;
	}
	
	

	public DBObject getLocation(String pin){
		DBCollection coll = db.getCollection("pin_master");
		
		BasicDBObject fields = new BasicDBObject();
		fields.put("pincode", pin);
		DBCursor dbCursor = coll.find(fields);
		DBObject dbObject = null;
		while(dbCursor.hasNext()){
			dbObject = dbCursor.next();
		}
		
		return dbObject;
	}
	
	public DBObject getProduct(String sku){
		DBCollection coll = db.getCollection("products_master");
		
		BasicDBObject fields = new BasicDBObject();
		fields.put("sku", sku);
		DBCursor dbCursor = coll.find(fields);
		DBObject dbObject = null;
		while(dbCursor.hasNext()){
			dbObject = dbCursor.next();
		}
		return dbObject;
	}
	
	public static void inAppNotification(org.codehaus.jettison.json.JSONObject jsonObj){
		DBCollection coll = db.getCollection("inApp_notify");
		System.out.println("Sending to MongoDB"+jsonObj.toString());
		DBObject dbObj =(DBObject)JSON.parse(jsonObj.toString());
		coll.insert(dbObj);
	}
	
	public void setEvent(String userId){
		
	}
	/*public static void main(String args[]) throws Exception{
		DBObject obj = new MongoDBQuery().getUser("080060cb-5111-J02S-a96e-7d6df4228cbe");
		System.out.println(obj.get("email")+" "+obj.get("name"));
	}*/
}
