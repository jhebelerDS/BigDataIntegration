package accumuloExpore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

public class AccumuloExplore {

	
	public void main() throws AccumuloException, AccumuloSecurityException{
	 Instance instances = new ZooKeeperInstance("fish", "54.197.236.191");
	 AuthenticationToken token = new PasswordToken("123456789");
	 Connector conn = instances.getConnector("root",token);
	 
	
 	}
}
