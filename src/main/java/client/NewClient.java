package client;

// import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.GlobalStateIdContext;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.ipc.RPC;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
public class NewClient {
	/*
	private final RPC.Server clientRpcServer;
	private final FSNamesystem namesystem;
	public newClient(Configuration conf){
		this.clientRpcServer = new RPC.Builder(conf)
				.setProtocol(
						org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
				.setInstance(clientNNPbService).setBindAddress(bindHost)
				.setPort(rpcAddr.getPort()).setNumHandlers(handlerCount)
				.setVerbose(false)
				.setSecretManager(namesystem.getDelegationTokenSecretManager())
				.setAlignmentContext(stateIdContext)
				.build();
	}*/
	public static void main(String[] args) throws Exception{

		BasicConfigurator.configure();
		System.setProperty("hadoop.home.dir", "/");
		System.out.println("NewClient");
		Configuration conf = new HdfsConfiguration();
		System.out.println("12");
		conf.set("dfs.namenode.name.dir", "/hadoop/hdfs/name");
		// FSNamesystem nameSystem = new FSNamesystem(conf, null, true);
		System.out.println("14");
		FSNamesystem fsn = FSNamesystem.loadFromDisk(conf);
		System.out.println("16");


		// TODO: close is not a public function, need to change it to public
		// want to test if the fsn matters with the program not terminating
		//fsn.close();
		/*
		ClientNamenodeProtocolServerSideTranslatorPB
				clientProtocolServerTranslator =
				new ClientNamenodeProtocolServerSideTranslatorPB(this);
		BlockingService clientNNPbService = ClientNamenodeProtocol.
				newReflectiveBlockingService(clientProtocolServerTranslator);
		GlobalStateIdContext stateIdContext = new GlobalStateIdContext((fsn));
		RPC.Server clientRpcServer = new RPC.Builder(conf)
				.setProtocol(
						org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
				.setInstance(clientNNPbService).setBindAddress("35.3.80.176")
				.setPort(8001).setNumHandlers(1)
				.setVerbose(false)
				.setSecretManager(fsn.getDelegationTokenSecretManager())
				.setAlignmentContext(stateIdContext)
				.build();
		 */
		GlobalStateIdContext stateIdContext = new GlobalStateIdContext((fsn));
		System.out.println("51");
		fsn.close();
	}
}