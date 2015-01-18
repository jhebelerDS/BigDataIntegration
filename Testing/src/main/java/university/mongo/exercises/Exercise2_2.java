package university.mongo.exercises;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;

public class Exercise2_2 {

	public static void main(String[] args) throws UnknownHostException {
		MongoClient mongoDBClient = new MongoClient();
		DB db = mongoDBClient.getDB("students");
		DBCollection coll = db.getCollection("grades");
		
		DBCursor cur = coll.find();
		
		System.out.println("Count: " + cur.count());
		
		DBCursor curQuiz = coll.find(new BasicDBObject("type","homework"))
				           .sort(new BasicDBObject("student_id", -1).append("score", -1));
		
		
		String prevStudentId = null;
		DBObject prevRow = null;
		
		while(curQuiz.hasNext()){
			DBObject row = curQuiz.next();
			String answer = row.get("student_id").toString();
			if( prevStudentId == null ){
				prevStudentId = answer;
			} else if( !prevStudentId.equals(answer)){
				// Delete the previous row
				//System.out.println("Delete prevRow: " + prevRow.toString());
				DBCursor curTmp = coll.find(new BasicDBObject("student_id",prevStudentId)
				.append("homework", prevRow.get("score").toString()));
				
				System.out.println("GOING TO DELETE: " + curTmp.toString());
				
				WriteResult rs = coll.remove(new BasicDBObject("_id",prevRow.get("_id")));
				
				System.out.println("Rows Affected: " + rs.getN());
			}
		    prevRow = row;
		    prevStudentId = answer; 
		}
		
		// Get rid of the last one
		WriteResult rs = coll.remove(new BasicDBObject("_id",prevRow.get("_id")));
		
		System.out.println("Rows Affected: " + rs.getN());

		
		System.out.println("Remaining: " + coll.find().count());
	}

}
