package university.mongo.exercises;

import java.net.UnknownHostException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;

public class Exercise3_1{

	public static void main(String[] args) throws UnknownHostException {
		MongoClient mongoDBClient = new MongoClient();
		DB db = mongoDBClient.getDB("school");
		DBCollection coll = db.getCollection("students");
		
		DBCursor curQuiz = coll.find();
		
		System.out.println("Count: " + curQuiz.count());
		
	//	DBCursor curQuiz = coll.find();
		
		while(curQuiz.hasNext()){
			DBObject row = curQuiz.next();
			
			Object id = row.get("_id");
			
			BasicDBList scores = (BasicDBList) row.get("scores");
			
			float lowScore = 100;
			
			DBObject tmpScores = null;
			int entryToRemove = 100;
			for(int i=0; i< scores.size(); i++ ){
				tmpScores = (DBObject) scores.get(i);
				String type = tmpScores.get("type").toString();
				if( "homework".equals(type)){
				         System.out.println("\nValue: " + tmpScores.get("score").toString());
						if(Float.parseFloat(tmpScores.get("score").toString()) < lowScore){
			                 entryToRemove = i;
			                 lowScore = Float.parseFloat(tmpScores.get("score").toString());
				         }
					}
			}
			
			scores.remove(entryToRemove);
			//row.
			//row.removeField("scores");
			row.put("scores" , scores);
			WriteResult rs = coll.update(new BasicDBObject("_id", id), row);
			System.out.println("RESULT: " + rs.getN());
			
			//System.out.println("REMOVEING: " + entryToRemove);
			//row.removeField("scores");
//			if( prevStudentId == null ){
//				prevStudentId = answer;
//			} else if( !prevStudentId.equals(answer)){
//				// Delete the previous row
//				//System.out.println("Delete prevRow: " + prevRow.toString());
//				DBCursor curTmp = coll.find(new BasicDBObject("student_id",prevStudentId)
//				.append("homework", prevRow.get("score").toString()));
//				
//				System.out.println("GOING TO DELETE: " + curTmp.toString());
//				
//				WriteResult rs = coll.remove(new BasicDBObject("_id",prevRow.get("_id")));
//				
//				System.out.println("Rows Affected: " + rs.getN());
//			}
//		    prevRow = row;
//		    prevStudentId = answer; 
		}
//		
//		// Get rid of the last one
//		WriteResult rs = coll.remove(new BasicDBObject("_id",prevRow.get("_id")));
//		
//		System.out.println("Rows Affected: " + rs.getN());
//
//		
//		System.out.println("Remaining: " + coll.find().count());
	}

}
