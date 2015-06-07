package umbc.hebeler.phd.bigdata;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class ProbeMongoDB implements Probe {
	String URI = "http://edu.umbc.hebeler.phd.probe/0415#";
	String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

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

	public int extractStructures() {
		
		List<String> databases = mongoClient.getDatabaseNames();
		Random rand = new Random();
		int skipRecords = 0;

		// First look at each database
		for(String databaseString:databases){
			DB database = mongoClient.getDB(databaseString);
			Set<String> collections = database.getCollectionNames();
			
			//Now look at each collection (table)
			for(String collectionString: collections){
				// Open the collection and get the number of rows
				DBCollection collection = database.getCollection(collectionString);
				int numRecords = (int)collection.count();
				// Need to test that the return fits into an int!!
				
				if(numRecords <=0 ) continue;
				
				// Calculate the number of samples and size
				if( numRecords < BATCHSIZE){
					// Exhaustive approach
					System.out.println("Exhaustive missed : " + collectionString);
				} else {
					System.out.println("Exhaustive matched : " + collectionString);

					//DBCursor cursor = collection.find();
					//cursor.batchSize(BATCHSIZE);
					
			  		DBStructure structure = new DBStructure(); 
					
					for(int j = 0; j<MAXSAMPLES  ; j++){
						
					   // Get a number within the range of the collection
						DBCursor cursor = collection.find();
				
						skipRecords = rand.nextInt(numRecords-BATCHSIZE)  ;
						cursor.skip(skipRecords);

						DBObject obj = cursor.next();
					     // Step through DB objects
						if (structure.getStructures().size() == 0) {
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
					// Copy structure into arraylist
					structures.add(structure);
				}
			}
		}

		return structures.size();
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

	public Model convert2RDF()  {
		InputStream ontology = null;
		// Examine structures and make 
		Model m = ModelFactory.createOntologyModel();
		
		try {
			// Get current working environment
			final String dir = System.getProperty("user.dir");
	        System.out.println("current dir = " + dir);
			ontology = new FileInputStream("src/main/resources/noSQLProbe.rdf");
		} catch (FileNotFoundException e) {
			System.out.println("File Not Found");
			return null;
		}
		
		System.out.println("Reading NoSql Probe");
		m.read(ontology,"RDF/XML");

		// Add statements
		//Step through the arraylist converting as necessary
		Resource currentDatabase = null;
		Resource currentTable = null;
		
		for(DBStructure dbs:structures){
			// Setup the database and assocated table
			Resource database = m.createResource();
			Resource table = m.createResource();
			Literal databaseName = m.createLiteral(dbs.getDbName());
			Literal tableName = m.createLiteral(dbs.getTableName());
			// First see if database is new
			Resource databaseObj = null;
			Resource endObj = null;
			if(!Objects.equals(currentDatabase, database)){
				// Create database object
				databaseObj = m.createResource(URI+  databaseName);
				Property prot = m.createProperty(rdf, "type");
				Resource databaseClass = m.getResource("http://edu.umbc.hebeler.phd.probe/0415#Database");
				m.add(databaseObj, prot, databaseClass);
			}
			// Next see if table is new
			if(!Objects.equals(currentTable, table)){
				// Create table object
				databaseObj = m.createResource(URI+  tableName);  
				Property prot = m.createProperty(rdf, "type");
				Resource databaseClass = m.getResource("http://edu.umbc.hebeler.phd.probe/0415#Table");
				m.add(databaseObj, prot, databaseClass);
				// Connect the table to the database
			}
			//  Now go through each record
			for(Object d: dbs.getStructures()){
			    DBObject dbo = (DBObject) d;
				Map saveObjMap = dbo.toMap();
				Set saveObjSet = saveObjMap.keySet();
				Iterator saveObjIt = saveObjSet.iterator();
				while(saveObjIt.hasNext()){
					// Allocate one record structure for each element				databaseObj = m.createResource(URI+  tableName);  
					endObj = m.createResource(URI+  saveObjIt.next().toString());  
					Property prot = m.createProperty(rdf, "type");
					Resource databaseClass = m.getResource("http://edu.umbc.hebeler.phd.probe/0415#Row");
					m.add(endObj, prot, databaseClass);

				}

			}
			
		}
		
		return m;
	}

	public boolean populate() {
		
	    mongoClient.dropDatabase("testDB");
		
		DB db = mongoClient.getDB("testDB");
		
		DBCollection coll = db.getCollection("testCollection");
		
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
		return true;
	}

}
