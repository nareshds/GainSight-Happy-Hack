package storm.starter.spout;


import java.net.UnknownHostException;

import org.bson.BSONObject;
import org.codehaus.jettison.json.JSONObject;
import org.json.simple.JSONArray;

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
	
	public DBObject getEvent(String userId){
		DBCollection coll = db.getCollection("notification_table");
		
		BasicDBObject fields = new BasicDBObject();
		fields.put("id", userId);
		DBCursor dbCursor = coll.find(fields);
		DBObject dbObject = null;
		while(dbCursor.hasNext()){
			dbObject = dbCursor.next();
			System.out.println("+++++Retreving form created table+++++");
			System.out.println(dbObject.get("time"));
		}
		return dbObject;
	}
	
	public static void inAppNotification(org.codehaus.jettison.json.JSONObject jsonObj){
		DBCollection coll = db.getCollection("inApp_notify");
		System.out.println("Sending to MongoDB"+jsonObj.toString());
		DBObject dbObj =(DBObject)JSON.parse(jsonObj.toString());
		coll.insert(dbObj);
	}
	
	
	public void setEvent(org.codehaus.jettison.json.JSONObject eventObj){
		
		DBCollection coll = db.getCollection("notification_table");
		System.out.println(db.getCollectionNames());
		try{
		BasicDBObject fields = new BasicDBObject();
		fields.put("id", eventObj.get("id"));
		DBCursor cursor = coll.find(fields);
		if(cursor.length() == 0){
			DBObject dbObj =(DBObject)JSON.parse(eventObj.toString());
			coll.insert(dbObj);
		}else{
			while(cursor.hasNext()){
				 DBObject update = cursor.next();
				 coll.insert((update));
				 if(update.get("id") == null) System.out.println("is Null");
				 else System.out.println(update);
				 update.putAll((BSONObject) eventObj);
				 JSONArray timeArray = (JSONArray) update.get("time");
				 if(timeArray == null) timeArray = new JSONArray();
				 timeArray.add(eventObj.get("time"));
				 update.put("time", timeArray);
				 
				 JSONArray eventArray = (JSONArray) update.get("event");
				 if(eventArray == null) eventArray = new JSONArray();
				 eventArray.add(eventObj.get("event"));
				 update.put("event", eventArray);
				 
				 JSONArray skuArray = (JSONArray) update.get("sku");
				 if(skuArray == null) eventArray = new JSONArray();
				 skuArray.add(eventObj.get("sku"));
				 update.put("sku", skuArray);
				 
				 JSONArray priceArray = (JSONArray)update.get("price");
				 if(priceArray == null) priceArray = new JSONArray();
				 priceArray.add(eventObj.get("price"));
				 update.put("price", priceArray);
				 
				 JSONArray searchArray = (JSONArray)update.get("serach string");
				 if(searchArray == null) searchArray = new JSONArray();
				 searchArray.add(eventObj.get("search string"));
				 update.put("search string", searchArray);
			 }
			System.out.println(db.getCollectionNames());
		}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
	}
	public void setUserCountInGeography(org.codehaus.jettison.json.JSONObject eventObj) throws Exception{
		DBCollection coll = db.getCollection("geographywise_hit");
		
		BasicDBObject fields = new BasicDBObject();
		MongoDBQuery mongoDBQuery=new MongoDBQuery();
		fields.put("id", eventObj.get("userId"));
		fields.put("pincode",mongoDBQuery.getUser(eventObj.get("userId").toString()).get("pincode"));
		DBObject pinCodeObject= mongoDBQuery.getLocation(mongoDBQuery.getUser(eventObj.get("userId").toString()).get("pincode").toString());
		int count=0;
		
		
		DBCursor cursor = coll.find(fields);
		 while(cursor.hasNext()){
			 DBObject update = cursor.next();
			 
			 JSONArray timeArray = (JSONArray) update.get("time");
			 if(timeArray == null) timeArray = new JSONArray();
			 timeArray.add(eventObj.get("time"));
			 update.put("time", timeArray);
			 
			 JSONArray eventArray = (JSONArray) update.get("event");
			 if(eventArray == null) eventArray = new JSONArray();
			 eventArray.add(eventObj.get("event"));
			 update.put("event", eventArray);
			 
			 JSONArray skuArray = (JSONArray) update.get("pincode");
			 if(skuArray == null) eventArray = new JSONArray();
			 skuArray.add(eventObj.get("pincode"));
			 update.put("pincode", skuArray);
			 
			 JSONArray priceArray = (JSONArray)update.get("disrictname");
			 if(priceArray == null) priceArray = new JSONArray();
			 priceArray.add(eventObj.get("disrictname"));
			 update.put("disrictname", priceArray);
			 
			 JSONArray searchArray = (JSONArray)update.get("statename");
			 if(searchArray == null) searchArray = new JSONArray();
			 searchArray.add(eventObj.get("statename"));
			 update.put("statename", searchArray);
		 }
	}



	public Integer getuserVistCount(String userId) {
		DBCollection coll = db.getCollection("notification_table");
		BasicDBObject fields = new BasicDBObject();
		fields.put("id", userId);
		DBCursor dbCursor = coll.find(fields);
		DBObject dbObject = null;
		Integer noOfVists = 0;
		while(dbCursor.hasNext()){
			dbObject = dbCursor.next();
			JSONArray timeArray = (JSONArray)dbObject.get("time");
			noOfVists = timeArray.size();
		}
		return noOfVists;
	}
	
	/*public static void main(String args[]) throws Exception{
		new MongoDBQuery().setEvent(new JSONObject("{\"id\":\"32628068-724c-Jb0S-9efd-d9a102ae1e8f\",\"email\":\"32628068@gainsighttest.com\",\"gender\":\"F\",\"pincode\":741201,\"dob\":\"1978-04-12T15:57:24.947Z\",\"signUpDate\":\"2010-01-01T05:38:22.010Z\",\"name\":\"User - 557900\"}"));
		System.out.println("ENd");
	}*/
}
