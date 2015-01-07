package MavenPlay.Testing;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.MongoClient;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws UnknownHostException
    {
        System.out.println( "Hello World! " );
		MongoClient mongoClient = new MongoClient();
		
		// Clear the DB
		mongoClient.dropDatabase("testDB");
		
		DB db = mongoClient.getDB("testDB");
		
		// coll = db.getCollection("testCollection");
	//
    }
}
