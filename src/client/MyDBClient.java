package client;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import server.SingleServer;
import server.MyDBReplicatedServer;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class should implement your DB client.
 */
public class MyDBClient extends Client {
    private NodeConfig<String> nodeConfig= null;

    private AtomicInteger i = new AtomicInteger(0);
    ConcurrentMap<Integer, Callback> callbacks= new ConcurrentHashMap<Integer, Callback>();

    public MyDBClient() throws IOException {
        super();
    }
    public MyDBClient(NodeConfig<String> nodeConfig) throws IOException {
        super();
        this.nodeConfig = nodeConfig;
    }

    @Override
    protected void handleResponse(byte[] bytes, NIOHeader header) {
        // expect echo reply by default here
    	try {
    	String response = new String(bytes, SingleServer.DEFAULT_ENCODING);
    	String responseFromClient = "";
        Integer responseId = null;
    	if(response.contains(":id1:")) {
    		String[] responseStrings = response.split(":id1:"); 
    		responseFromClient = responseStrings[1];
    		responseId = Integer.parseInt(responseStrings[2]);
    		Callback cb = callbacks.get(responseId);
    		cb.handleResponse(bytes, header);
    	}
    	else
    	{
    		Callback cb = new CallbackImplementer();
    		cb.handleResponse(bytes, header);
    	}
       }catch(UnsupportedEncodingException e) {
    	   e.printStackTrace();
       }
    }
    
    
    @Override
    public void callbackSend(InetSocketAddress isa, String request, Callback
            callback) throws IOException {
                Integer request_id = i.incrementAndGet();
                callbacks.put(request_id, callback);
                request = request+":id:"+Integer.toString(request_id);
                String[] a = request.split(":id:");
                this.send(isa, request);  
        }

    //test callback implementer
    static class CallbackImplementer implements Callback{
    
        @Override
        public void handleResponse(byte[] bytes, NIOHeader header) {
            try {
                 System.out.println(new String(bytes, SingleServer.DEFAULT_ENCODING));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

    }
    
    //test main
    public static void main(String[] args) throws IOException {

    	MyDBClient myDBClient = new MyDBClient();
        // insert a record first with an empty list
    	Callback callBack = new CallbackImplementer();
    	 myDBClient.callbackSend(MyDBReplicatedServer.getSocketAddress(args), "CREATE TABLE emptable10701234(\r\n"
    	 		+ "   emp_id int PRIMARY KEY,\r\n"+ "   );", callBack );
    	myDBClient.callbackSend(MyDBReplicatedServer.getSocketAddress(args), "describe tables;", callBack );
   	// myDBClient.callbackSend(MyDBReplicatedServer.getSocketAddress(args), "show tables;", callBack );
    // 	 myDBClient.callbackSend(MyDBReplicatedServer.getSocketAddress(args), "CREATE TABLE emptable0002(\r\n"
    //  	 		+ "   emp_id int PRIMARY KEY,\r\n"+ "   );", callBack );
    // 	 myDBClient.callbackSend(MyDBReplicatedServer.getSocketAddress(args), "CREATE TABLE emptable0003(\r\n"
    //  	 		+ "   emp_id int PRIMARY KEY,\r\n"+ "   );", callBack );


    }
}