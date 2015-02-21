package course;

import java.net.UnknownHostException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.DBObject;


public class PhotoRemove {

	public static void main(String[] args) throws UnknownHostException {
        MongoClient c =  new MongoClient(new MongoClientURI("mongodb://localhost"));
        DB db = c.getDB("photo");
        DBCollection albums = db.getCollection("albums");
        DBCollection images = db.getCollection("images");
        
        DBCursor dc = images.find();
        DBCursor al = null;
       // int count = 100;
        while(dc.hasNext()){
        	DBObject image= dc.next();
        	//System.out.println("IMAGE: " + image.toString());
        	BasicDBList lis = new BasicDBList();
         	//System.out.println("ID: " + image.get("_id"));
         	lis.add(image.get("_id"));
         	
        	// Search for image in albums
        	al = albums.find(new BasicDBObject("images", new BasicDBObject("$in",lis )));
        	if( al.count() == 0){
        		//remove this one
        		System.out.print(".");
        		images.remove(image);
        	}
        	//if ( count-- <= 0 )break;
        }
        
        dc.close();
        al.close();

	}

}
