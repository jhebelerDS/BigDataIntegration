package accumuloExpore;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class AccumuloExplore {

	
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException{

	 // Connect	
	 Instance instances = new ZooKeeperInstance("accumulo", "accumuloServer");
	 AccumuloConfiguration ac = instances.getConfiguration();
	 AuthenticationToken token = new PasswordToken("123456789");
	 Connector conn = instances.getConnector("root",token);
	 
	 conn.tableOperations().create("table");
	 
	 // Write
	 BatchWriter wr = conn.createBatchWriter("table", new BatchWriterConfig());
	 
	 Mutation m = new Mutation(new Text("12345"));
	 Text family = new Text("family");
	 Text qual = new Text("qual");
	 Value value = new Value("FE".getBytes());
	 
	 m.put(family, qual, new ColumnVisibility("public"), value);
	 
	 wr.addMutation(m);
	 
	 wr.close();
	 
	 // Read
	 
	 Authorizations au = new Authorizations("public");
	 Scanner scanner = conn.createScanner("table", au);
	 
	 Range range = Range.prefix("12345");
	 
	 scanner.setRange(range);
	 
	 Text familyIn = null, qualIn = null, idIn = null;
	 Value valueIn = null;
	 String visibility = null;
	 
	 for(Entry<Key, Value> entry: scanner){
		 idIn = entry.getKey().getRow();
		 familyIn = entry.getKey().getColumnFamily();
		 qualIn = entry.getKey().getColumnQualifier();
		 valueIn = entry.getValue();
		 visibility = entry.getKey().getColumnVisibility().toString();
		 System.out.println("Id= " + idIn.toString() + " Family= " + familyIn + 
				            " Qual= " + qualIn.toString() + " Value: " + valueIn.toString());
		 System.out.println("Visibility: " + visibility + " Time: " + entry.getKey().getTimestamp());
 
	 }
	 scanner.close();	
 	}
}
