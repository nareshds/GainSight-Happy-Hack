package storm.starter.spout;

public class NoOfVisits {
	public Integer userVists(String userId){
		return new MongoDBQuery().getuserVistCount(userId);
	}
}
