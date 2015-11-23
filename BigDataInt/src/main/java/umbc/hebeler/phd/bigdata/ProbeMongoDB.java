package umbc.hebeler.phd.bigdata;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;

import com.hp.hpl.jena.ontology.DatatypeProperty;
import com.hp.hpl.jena.ontology.ObjectProperty;
import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.ontology.Ontology;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class ProbeMongoDB implements Probe {
	String URIstructure = "http://edu.umbc.hebeler.phd.structure/0415#";
	String URIdomain = "http://edu.umbc.hebeler.phd.domain/0915#";
	String URIdb = null;
	String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

	MongoClient mongoClient = null;
	ArrayList<DBStructure> structures = new ArrayList<DBStructure>();


	public boolean connect(String urlRaw, String user, String pw) {
		URL url = null;
		
		try {
			// Just localhost for default
			try {
				url = new URL(urlRaw);
				
			} catch (MalformedURLException e) {
				System.out.println("Poorly formed URL: " + urlRaw);
			}
			 mongoClient = new MongoClient();
			 
			 // Good enough for now but must consider getting the port number for multiple instances on same URL
			 String port = (url.getPort() != -1)? ("/" + Long.toString(url.getPort())):"";
			 URIdb = url.getProtocol()+ "://"+ url.getHost() + port + "/mongo#";
			 
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
			  		structure.setRowCount(numRecords);
					
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
								   // Here is where you need to save the structure of a document
								   // Read one document and decompose it
								   //obj.toMap();
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

	public OntModel convert2RDF()  {
		InputStream ontology = null;
		// Examine structures and make 
		OntModel m = ModelFactory.createOntologyModel();
		//Resource on = m.createOntology("http://www.semanticweb.org/jhebeler/ontologies/2015/8/untitled-ontology-8");
		
		//    <owl:Ontology rdf:about="http://www.semanticweb.org/jhebeler/ontologies/2015/8/untitled-ontology-8"/>

		//Dataset ds = TDBFactory.createDataset("/Users/jhebeler/TDBStore2");
		PrefixMap pm = PrefixMapFactory.create();
		
		
		
        //Model specificModelStore = ds.getNamedModel("mongo12");
        
		
		try {
			// Get current working environment
			final String dir = System.getProperty("user.dir");
	        System.out.println("current dir = " + dir);
			ontology = new FileInputStream("foundation.owl");
		} catch (FileNotFoundException e) {
			System.out.println("File Not Found");
			return null;
		}
		
		System.out.println("Reading Foundation");
		m.read(ontology,"RDF/XML");
  
		// Add statements
		//Step through the arraylist converting as necessary
		String currentDatabase = null;
		String database = null;
		String currentTable = null;
		String table = null;
		
		//Set up standard class references
		// Database
		
		OntClass databaseClass = m.createClass(URIstructure+"Database");
		OntClass domainClass = m.createClass(URIdomain+"Domain");

		// Table
		OntClass tableClass = m.createClass(URIstructure+"Table");
		// Record
		OntClass recordClass = m.createClass(URIstructure+"Record");
		//Instance Value
		OntClass instanceClass = m.createClass(URIstructure+"Instance");
		OntClass counter = m.createClass(URIstructure +"Count");
		
		
		// Set up standard properties references
		// hasDatabase
		ObjectProperty hasDatabase = m.createObjectProperty(URIstructure+"hasDatabase");
		// hasTable
		ObjectProperty hasTable = m.createObjectProperty(URIstructure+"hasTable");
		// hasRecord
		ObjectProperty hasRecord = m.createObjectProperty(URIstructure+ "hasRecord");
		//hasInstance
		ObjectProperty hasInstance =  m.createObjectProperty(URIstructure + "hasInstance");
		ObjectProperty hasMember = m.createObjectProperty(URIdomain + "hasMember");
		ObjectProperty hasCounter = m.createObjectProperty(URIdomain + "hasCounter");
		// hasName
		DatatypeProperty hasName =  m.createDatatypeProperty(URIstructure+ "hasName");
		// hasValue
		DatatypeProperty hasValue =  m.createDatatypeProperty(URIstructure + "hasValue");
		
//		m.cr
//		
		Property isType = m.createProperty(rdf, "type");
//		Property isAbout = m.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "about");
//		Resource ont = m.createResource("http://www.w3.org/2002/07/owl#Ontology");
//		
//		Ontology o = m.getOntology(uri);
//		o.
//		
//		m.add(ont, isAbout, on);
		
		
		Resource databaseInstance = null;
		Resource tableInstance = null;
		Resource recordInstance = null;
		Resource domainInstance = null;
		
		for(DBStructure dbs:structures){
			
			currentDatabase = dbs.getDbName();
			currentTable = dbs.getTableName();
			System.out.println("CURRENT TABLE: " + currentTable);
			
			
			// Set database object if new
			if(!Objects.equals(currentDatabase, database)){
				// Create database object
				databaseInstance = m.createResource(URIdb+  currentDatabase);
				m.add(databaseInstance, isType, databaseClass);
				// Set this up so we don't repeat above
				database = currentDatabase;
			}
			
			// Next see if table is new
			if(!Objects.equals(currentTable, table)){
				// Create table object
				tableInstance = m.createResource(URIdb +  currentTable);  
				m.add(tableInstance, isType, tableClass);
				table = currentTable;
				// Associate with database
				m.add(databaseInstance, hasTable, tableInstance);
				Resource counterInstance = m.createResource(URIdb + currentTable+ "counter");
				m.add(counterInstance, isType, counter);
				m.add(counterInstance, hasValue, Long.toString(dbs.getRowCount()));
				m.add(tableInstance, hasCounter, counterInstance);
	
				// Also set the table up as a class with instances for each row
				domainInstance = m.createResource(URIdomain + currentTable);
				m.add(domainInstance, isType, domainClass);
			
			}
			
			
			//  Now go through each record
			for(Object d: dbs.getStructures()){
			    DBObject dbo = (DBObject) d;
				Map saveObjMap = dbo.toMap();
				Set saveObjSet = saveObjMap.keySet();
				Iterator saveObjIt = saveObjSet.iterator();
				Collection instances = saveObjMap.values();
				Iterator instanceInterator = instances.iterator();
				while(saveObjIt.hasNext()){
					// Allocate one record structure for each element				databaseObj = m.createResource(URI+  tableName);  
					recordInstance = m.createResource(URIdb+  saveObjIt.next().toString());  
					//Resource databaseClass = m.getResource(URIdb);
					m.add(recordInstance, isType, recordClass);
					m.add(domainInstance, hasMember, recordInstance);
					// Connect each record to the specific table
					m.add(tableInstance,hasRecord, recordInstance );
					// Connect each record to the specific domain
					//m.add(recordInstance, isType, domainClass);
					
					// Add an instance
					// Declare as an instance
					String instanceV = instanceInterator.next().toString();
					if(instanceV.contains("{") ){
						instanceV = "UNPRINTABLE";
						continue;
					}
					String instanceValueStr = instanceV;
					System.out.println("INSTANCE VALUE: " + instanceValueStr);
					
					//String instanceValueStr = "http://edu.umbc.hebeler.phd.probe/0415#" + "JUNK";
					// NEED TO CREATE UNIQUE String relative to this table and 
					// Then Set Value as a data property so that it can contain anything.
					Resource instanceValue = m.createResource(instanceValueStr);
					m.add(instanceValue, isType, instanceClass);
					m.add(instanceValue, hasMember, recordInstance);
					
					// add to record as an example
					//m.add(instanceValue, pro, endObj);
					m.add(recordInstance,hasInstance,  instanceValue);
					//m.add(record)
					//m.add(instanceValue, hasInstance, )
				}

			}
			
		}
		// Add to overall model
		//specificModelStore.add(m);
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
		
		
		 coll = db.getCollection("PeopleTable");
		
		//  Populate DB
		
		for(int i = 0; i <1000 ; i++ ){
			
		   BasicDBObject doc = new BasicDBObject("PersonID", "Person" + i)
		     .append("sex", ((i%4)==0)?"female":"male")
		     .append("Age", i%80)
		     .append("Location", new BasicDBObject("x", 203+i).append("y", 102+ i));
		   coll.insert(doc);
		}
		
//		for(int i = 0; i <1000 ; i++ ){
//			
//			   BasicDBObject doc = new BasicDBObject("place", "fisbine" + i)
//			     .append("city", "trenton")
//			     .append("population", i+1000)
//			     .append("additional", new BasicDBObject("x1", 203+i).append("y1", 102+ i));
//			   coll.insert(doc);
//			}

		
		System.out.println("TOTAL RECORDS: " + coll.getCount());
		return true;
	}

}
