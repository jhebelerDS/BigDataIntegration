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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.bson.BSONObject;

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
		
		ArrayList<DBObject> structures = new ArrayList<DBObject>();
		ArrayList<DBObject> structuresTmp = new ArrayList<DBObject>();
		
		
		
		
		for(int sampNum = 1 ; sampNum < MAXSAMPLES+1; sampNum++){
			DBCursor cursor = coll.find();
			cursor.batchSize(BATCHSIZE);
			long numRecords = coll.count();
			System.out.println("NUMBER of RECORDS: " + numRecords);

			randomNum = rand.nextInt(1995);
			cursor.skip(randomNum);
			
			// You have your starting point - now print out five
			for(int j = 0; j<BATCHSIZE ; j++){
  			   DBObject obj = cursor.next();
			   System.out.println("SAMPLE " + sampNum + ": " + obj);
			   // Step through DB objects
			   if (structures.size() == 0) {
			   	   // Must seed the array
				   structures.add(obj);
			   } else {
				   // Figure out if this is the same as ANY of the saved structures
				   // Set flag to see if the same
				   boolean same = false;
				   for(DBObject saveObj: structures){
					   if( sameDBObjects(saveObj, obj) ){
						   same = true;
						   break;
					   }
				   }
				   if(!same){
					   structures.add(obj);
				   }
			   }
			
			}
		}
		
		System.out.println("Number of Structures Found: " + structures.size());
		
	}

	private boolean sameDBObjects(DBObject saveObj, DBObject obj) {
		boolean different = true;
		
		Map saveObjMap = saveObj.toMap();
		Set saveObjSet = saveObjMap.keySet();
		Iterator saveObjIt = saveObjSet.iterator();
		
		Map objMap = obj.toMap();
		Set objSet = objMap.keySet();
		if( objSet.isEmpty() ){
			return true;
		}
		Iterator objIt = objSet.iterator();
		
		while(saveObjIt.hasNext()){
			String compareKey = saveObjIt.next().toString();
			System.out.println("Comparing: " + compareKey );
			if(!obj.containsField(compareKey)){
				different = false;
				System.out.println("KEY NOT FOUND");
				break;
			}
		}
	    
		return different;

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
		
		for(int i = 0; i <1000 ; i++ ){
			
			   BasicDBObject doc = new BasicDBObject("place", "fisbine" + i)
			     .append("city", "trenton")
			     .append("population", i+1000)
			     .append("additional", new BasicDBObject("x1", 203+i).append("y1", 102+ i));
			   coll.insert(doc);
			}
		
		System.out.println("TOTAL RECORDS: " + coll.getCount());
		DBObject myDoc = coll.findOne();
		
		//System.out.println(myDoc);
		
	}

}
