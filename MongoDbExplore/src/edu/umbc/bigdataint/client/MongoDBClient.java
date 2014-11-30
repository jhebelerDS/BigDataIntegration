package edu.umbc.bigdataint.client;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;


public class MongoDBClient {
	
	DBCollection coll = null;

	public static void main(String[] args) throws UnknownHostException {
		MongoDBClient mongoDBClient = new MongoDBClient();
		
		mongoDBClient.randomFind();
		
		
	}
	
	private void randomFind() {
		
		int MAXSAMPLES = 5;
		Random rand = new Random();
		int randomNum = 0;
		int BATCHSIZE = 5;
		
		
		
		for(int sampNum = 1 ; sampNum < MAXSAMPLES+1; sampNum++){
			DBCursor cursor = coll.find();
			cursor.batchSize(BATCHSIZE);
			long numRecords = coll.count();
			System.out.println("NUMBER of RECORDS: " + numRecords);

			randomNum = rand.nextInt(95);
			cursor.skip(randomNum);
			
			DBObject obj = cursor.next();
			System.out.println("SAMPLE " + sampNum + ": " + obj);
		}
		
	}

	public MongoDBClient () throws UnknownHostException{
		MongoClient mongoClient = new MongoClient();
		
		// Clear the DB
		mongoClient.dropDatabase("testDB");
		
		DB db = mongoClient.getDB("testDB");
		
		 coll = db.getCollection("testCollection");
		
		//  Populate DB
		
		for(int i = 0; i <1000 ; i++ ){
			
		   BasicDBObject doc = new BasicDBObject("name", "MongoDB" + i)
		     .append("type", "database")
		     .append("count", i)
		     .append("info", new BasicDBObject("x", 203+i).append("y", 102+ i));
		   coll.insert(doc);
		}
		
		DBObject myDoc = coll.findOne();
		
		//System.out.println(myDoc);
		
	}

}
