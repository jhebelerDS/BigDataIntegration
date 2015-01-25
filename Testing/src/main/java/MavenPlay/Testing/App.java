package MavenPlay.Testing;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
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
		
		DB db = mongoClient.getDB("students");
		DBCollection coll = db.getCollection("grades");
		
		DBCursor cur = coll.find();
		
		System.out.println("Count: " + cur.count());
	
    }
}
