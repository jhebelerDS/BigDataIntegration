package umbc.hebeler.phd.bigdata;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.hp.hpl.jena.rdf.model.Model;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class ProbeMongoDB implements Probe {

	MongoClient mongoClient = null;
	ArrayList<DBStructure> structures = new ArrayList<DBStructure>();


	public boolean connect(String url, String user, String pw) {
		try {
			// Just localhost for default
			 mongoClient = new MongoClient();
		} catch (UnknownHostException e) {
		 return false;
		}
		return true;
	}

	public boolean extractStructures() {
		
		List<String> databases = mongoClient.getDatabaseNames();
		Random rand = null;
		long skipRecords = 0L;

		// First look at each database
		for(String databaseString:databases){
			DB database = mongoClient.getDB(databaseString);
			Set<String> collections = database.getCollectionNames();
			
			//Now look at each collection (table)
			for(String collectionString: collections){
				ArrayList<DBObject> structuresTmp = new ArrayList<DBObject>();
				// Open the collection and get the number of rows
				DBCollection collection = database.getCollection(collectionString);
				long numRecords = collection.count();
				// Calculate the number of samples and size
				if( numRecords < BATCHSIZE){
					// Exhaustive approach
				} else {
					DBCursor cursor = collection.find();
					cursor.batchSize(BATCHSIZE);
					
					// Get a number within the range of the collection
					while((skipRecords = rand.nextLong() ) >= numRecords - BATCHSIZE);
					// MUST FIX THIS FOR A LONG
					cursor.skip((int)skipRecords);
					
					for(int j = 0; j<BATCHSIZE ; j++){
			  			DBStructure structure = new DBStructure(); 
						DBObject obj = cursor.next();
					     // Step through DB objects
						if (structuresTmp.size() == 0) {
						   	 // Set up the DBstructure
							 structure.setDbName(databaseString);
							 structure.setTableName(collectionString);
							 structure.add(obj);
						 } else {
							 // Figure out if this is the same as ANY of the saved structures
							 // Set flag to see if the same
							 boolean same = false;
							 for(Object saveObj: structure.getStructures()){
								if( sameAs(saveObj, obj) ){
									 same = true;
									 break;
								}
							  }
							   if(!same){
								   structure.add(obj);
							   }
					
						  }
					
					}
				}
			}
		}

		return false;
	}

	public boolean sameAs(Object saveObj, Object obj) {
		    DBObject one = (DBObject) saveObj;
		    DBObject two = (DBObject) obj;
			boolean different = true;
			
			Map saveObjMap = one.toMap();
			Set saveObjSet = saveObjMap.keySet();
			Iterator saveObjIt = saveObjSet.iterator();
			
			Map objMap = two.toMap();
			Set objSet = objMap.keySet();
			if( objSet.isEmpty() ){
				return true;
			}
			Iterator objIt = objSet.iterator();
			
			while(saveObjIt.hasNext()){
				String compareKey = saveObjIt.next().toString();
				System.out.println("Comparing: " + compareKey );
				if(!two.containsField(compareKey)){
					different = false;
					System.out.println("KEY NOT FOUND");
					break;
				}
			}
		    
			return different;	
	}

	public Model convert2RDF() {
		// TODO Auto-generated method stub
		return null;
	}

}
