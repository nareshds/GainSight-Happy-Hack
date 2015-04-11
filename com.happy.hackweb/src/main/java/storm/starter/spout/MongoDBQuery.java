package storm.starter.spout;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDBQuery {
	private static MongoClient mongoClient = null;
	private static DB db =null;
	public MongoDBQuery() throws Exception{
		mongoClient = new MongoClient("192.168.0.76", 27017);
		db = mongoClient.getDB("gainsight");
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
	
	public void setEvent(String userId){
		
	}
	/*public static void main(String args[]) throws Exception{
		DBObject obj = new MongoDBQuery().getUser("080060cb-5111-J02S-a96e-7d6df4228cbe");
		System.out.println(obj.get("email")+" "+obj.get("name"));
	}*/
}
