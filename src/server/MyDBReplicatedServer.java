//package server;
//
//import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
//import edu.umass.cs.nio.MessageNIOTransport;
//import edu.umass.cs.nio.interfaces.NodeConfig;
//import edu.umass.cs.nio.nioutils.NIOHeader;
//import edu.umass.cs.nio.nioutils.NodeConfigUtils;
//
//import java.io.IOException;
//import java.net.InetSocketAddress;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.List;
//import java.util.Map;
//import java.util.PriorityQueue;
//import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.PriorityBlockingQueue;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.logging.Level;
//
//
//import org.apache.cassandra.cql3.Json;
//import org.json.JSONObject;
//
//
///**
// * This class should implement your replicated database server. Refer to
// * {@link ReplicatedServer} for a starting point.
// */
//public class MyDBReplicatedServer extends MyDBSingleServer {
//
//    Comparator<Message> messageLamportComparator = new Comparator<Message>() {
//
//        @Override
//        public int compare(Message o1, Message o2) {
//            // TODO Auto-generated method stub
//            if(o1.getLamport() > o2.getLamport()) {
//                return 1;
//            } else if (o2.getLamport() > o1.getLamport()) {
//                return -1;
//            } else {
//                return o1.getMessageSource().compareTo(o2.getMessageSource());
//            }
//        }
//        
//    };
//
//    Comparator<Message> messageIdComparator = new Comparator<Message>() {
//        @Override
//        public int compare(Message o1, Message o2) {
//            // TODO Auto-generated method stub
//            return o1.getMessageId() - o2.getMessageId(); 
//        }
//    };
//
//    public static final String SERVER_PREFIX = "server.";
//    public static final int SERVER_PORT_OFFSET = 1000;
//
//    protected final String myID;
//    protected final MessageNIOTransport<String,String> serverMessenger;
//
//    //This server's delivery queue which is Lamport clock ordered.
//    protected PriorityBlockingQueue<Message> deliveryQueue = new PriorityBlockingQueue<Message>(5,messageLamportComparator);
//    
//    //This server is storing all the messages that have not arrived in order for the server mentioned in the key of this map.
//    protected ConcurrentMap<String, PriorityQueue<Message>> buffer = new ConcurrentHashMap<String, PriorityQueue<Message>> ();
//    
//    //This server is expecting the next message ID to arrive from the server mentioned in key of this map.
//    protected ConcurrentMap<String, Integer> expectedMessageId = new ConcurrentHashMap<String, Integer> ();
//    
//    //Lamport Clock
//    public AtomicInteger lamportClock = new AtomicInteger(0);
//    
//    //this server's Message id tracker for messages from client
//    public AtomicInteger messageId = new AtomicInteger(0);
//    
//    //Key: message id (server##messageId), Value: comma separated server ids
//    public ConcurrentMap<String,String> acksReceived = new ConcurrentHashMap<String,String>();
//
//    //Setup
//    public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
//                                InetSocketAddress isaDB) throws IOException {
//            super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
//            nodeConfig.getNodePort(myID)-SERVER_PORT_OFFSET), isaDB, myID);
//            this.myID = myID;
//            this.serverMessenger = new
//            MessageNIOTransport<String, String>(myID, nodeConfig,
//            new AbstractBytePacketDemultiplexer() {
//                        @Override
//                        public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
//                            handleMessageFromServer(bytes, nioHeader);
//                            return true;
//                        }
//                    }, true);
//            for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
//                expectedMessageId.put(node, 1);
//                ////""tem.out.println("Initial expected message ids are: " + expectedMessageId);
//            } 
//            log.log(Level.INFO, "Server {0} started on {1}", new Object[]{this.myID, this.clientMessenger.getListeningSocketAddress()});
//    }
//
//    //Wrapper function to handle message from client
//    @Override
//    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
//        ////""tem.out.println("Entered handleMessageFromClient");
//        try {		
//    		handleReceiveMessageFromClient(new String(bytes, SingleServer.DEFAULT_ENCODING),header);
//    	}
//    	catch(Exception e) {
//    		e.printStackTrace();
//    	}
//    }
//
//    //handle message from client
//    private void handleReceiveMessageFromClient(String request, NIOHeader header) 
//    {
//        ////""tem.out.println("Entered handleReceiveMessageFromClient");
//        //Create message
//        synchronized(lamportClock){
//        Message m = new Message();
//        m.setIsAck(false);
//        m.setMessageId(messageId.incrementAndGet());
//        m.setQuery(request);
//        m.setMessageSource(this.myID);
//        m.setAckSource(this.myID);
//        m.setAckedMessageId(messageId.get());
//        m.setLamport(lamportClock.incrementAndGet());
//
//        // ////""tem.out.println("Received message from: host(" + header.sndr.getHostName() + ")" + " port(" + header.sndr.getPort() + ")");
//        String clientIpString = header.sndr.getHostName() + ":" + header.sndr.getPort();
//        m.setClientIP(clientIpString);
//
//        //put message in delivery queue of this server
//        this.deliveryQueue.put(m);
//        
//        //m.setLamport(lamportClock.incrementAndGet());
//
//        JSONObject jsonMessage = new JSONObject(m);
//
//        //System.out.println("Received messages from client "+jsonMessage);
//        ////""tem.out.println("Message recieved from client before multicasting: " + jsonMessage.toString());
//
//        System.out.println("source: " + this.myID + "  " + jsonMessage);
//        multicast(jsonMessage.toString());
//    }
//
//    }
//
//    // Wrapper to handle messages from other servers.
//    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
//        ////""tem.out.println("Entered handleMessageFromServer");
//        try{
//
//            String message = new String(bytes, SingleServer.DEFAULT_ENCODING);
//            JSONObject messageJsonObject = new JSONObject(message);
//            
//            System.out.println(this.myID + " Recieved message: " + messageJsonObject);
//            System.out.println(this.myID + "My server has Received acks: " + this.acksReceived);
//
//            //received an ack
//            if(messageJsonObject.getBoolean("isAck"))
//            {
//                handleReceiveAckFromServer(messageJsonObject);
//            }
//            //Recieved a multicast message
//            else  
//            {
//                handleReceiveMulticastFromServer(messageJsonObject);
//            }
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//        log.log(Level.INFO, "{0} received relayed message from {1}",
//                new Object[]{this.myID, header.sndr}); // simply log
//    }
//
//
//    //Handles multicast messages sent to this server by other servers
//    private void handleReceiveMulticastFromServer(JSONObject messageJsonObject) 
//    {
//        ////""tem.out.println("Entered handleReceiveMulticastFromServer");
//    	synchronized(lamportClock) {
//    	try {
//        // System.out.println(this.myID+ " : " + messageJsonObject);
//
//    	// ////""tem.out.println("multicast recieved from server: " + messageJsonObject.toString());
//        String messageSource = messageJsonObject.getString("messageSource");
//        //Implementing underlying FIFO by buffering messages that arrive not in order from the other servers
//        // System.out.println("printing check at my id "+this.myID+" received message id "+messageJsonObject.getInt("messageId")+" expected message id "+this.expectedMessageId.get(messageSource));
//        if(messageJsonObject.getInt("messageId") != this.expectedMessageId.get(messageSource)){
//        //   System.out.println("inserting in buffer at my id "+this.myID+" received message id "+messageJsonObject.getInt("messageId"));
//            if(this.buffer.get(messageSource) == null ) {
//                //""tem.out.println(messageIdComparator);
//                PriorityQueue<Message> sourceBuffer = new PriorityQueue<Message>(5, messageIdComparator);
//                sourceBuffer.add(createMessageFromJSON(messageJsonObject));
//                //""tem.out.println(sourceBuffer);
//                this.buffer.put(messageSource,sourceBuffer);
////                System.out.println("inside if buffer "+this.buffer);
//            }
//            else {
//                PriorityQueue<Message> sourceBuffer = buffer.get(messageSource);
//                sourceBuffer.add(createMessageFromJSON(messageJsonObject));
//                this.buffer.put(messageSource, sourceBuffer);
////                System.out.println("inside else buffer "+this.buffer);
//
//            }
//        }
//        //Message received is the expected message from the other server so process it
//        else {
//        	// System.out.println("inserting into delivery queue my id "+this.myID+" received message id "+messageJsonObject.getInt("messageId"));
//            updateLamportClock(messageJsonObject.getInt("lamport"));
//            //Updating acks for sender in this server to check for head all acks
//            updateACKS(messageJsonObject);
//            // ////""tem.out.println(this.myID + ":" + acksReceived);
//            Message m = createMessageFromJSON(messageJsonObject);
//            // System.out.println(this.myID+ " 213: " + m);
//            this.deliveryQueue.put(m);
//            Message ack = createAckMessageAndUpdateLocalClock(messageJsonObject);
//            JSONObject jsonAck = new JSONObject(ack);
//            // ////""tem.out.println("ack from server: " +jsonAck.toString());
//            multicast(jsonAck.toString());
//            //expecting the next message from the sender.
//            expectedMessageId.put(messageSource, expectedMessageId.get(messageSource)+1);
//            // System.out.println("printing outer delivery queue at server "+this.myID+" "+this.deliveryQueue);
//            //Next message from sender has been buffered.
//            while(buffer.get(messageSource) != null && !buffer.get(messageSource).isEmpty() && buffer.get(messageSource).peek().getMessageId() == expectedMessageId.get(messageSource))
//            {
//                Message m1 = buffer.get(messageSource).poll();
//                
//                //If message is a multicast message then process it.
//                if(!m1.getIsAck()) {
//                    JSONObject m1JsonObject = new JSONObject(m1);
//                    updateLamportClock(m1JsonObject.getInt("lamport"));
//                    //Updating acks for sender in this server to check for head all acks
//                    updateACKS(m1JsonObject);
//                    this.deliveryQueue.put(m1);
//                    Message ackm1 = createAckMessageAndUpdateLocalClock(m1JsonObject);
//                    JSONObject jsonAckm1 = new JSONObject(ackm1);
//                    // ////""tem.out.println("ack from server: " +jsonAck.toString());
//                    multicast(jsonAckm1.toString());
//                    //Expecting next message from sender.
//                    expectedMessageId.put(messageSource, expectedMessageId.get(messageSource)+1);
//                    // System.out.println("printing inner delivery queue at server "+this.myID+" "+this.deliveryQueue);
//                }
//                else {
//                    //Message is an ack message and needs to be handled separately.
//                    handleReceiveAckFromServer(new JSONObject(m1));
//                }
//            }
//        }
//    	} catch(Exception e)
//    	{
//    		e.printStackTrace();
//    	}
//
//    //    System.out.println("printing buffer at my id "+this.myID+" "+this.buffer);
//    for (Map.Entry<String, PriorityQueue<Message>> entry : buffer.entrySet()) {
//    String serverId = entry.getKey();
//    PriorityQueue<Message> messageQueue = entry.getValue();
//
//    // System.out.println("Buffer at my id " + this.myID+" printing server id "+serverId);
//    // System.out.println("Messages in Buffer for " + serverId + ":");
//
//    for (Message message : messageQueue) {
//        // System.out.println("butter at my id : " + this.myID+" printing message id "+message.getMessageId() + ", Lamport Timestamp: " + message.getLamport());
//    }
//}
//    	}
//    }
//
//
//    //Handles ack messages sent to this erver by other servers.
//     private void handleReceiveAckFromServer(JSONObject messageJsonObject)
//    {
//        ////""tem.out.println("Entered handleReceiveAckFromServer");
//    	try {
//            synchronized(deliveryQueue){
//    	// ////""tem.out.println("Ack recieved from server: " + messageJsonObject);
//        String messageSource = messageJsonObject.getString("messageSource");
//
//        //Implementing underlying FIFO by buffering messages that arrive not in order from the other servers
//        System.out.println("checking acks at my id "+this.myID+" message source: "+messageJsonObject.getString("messageSource")+"ack source: "+messageJsonObject.getString("ackSource")+" acked message id "+messageJsonObject.getInt("ackedMessageId"));
//
//        // if(messageJsonObject.getInt("messageId") != expectedMessageId.get(messageSource)){
//        //     if(buffer.get(messageSource) == null ) {
//        //         PriorityQueue<Message> sourceBuffer = new PriorityQueue<Message>(5, messageIdComparator);
//        //         sourceBuffer.add(createMessageFromJSON(messageJsonObject));
//        //         buffer.put(messageSource,sourceBuffer);
//        //     }
//        //     else {
//        //         PriorityQueue<Message> sourceBuffer = buffer.get(messageSource);
//        //         sourceBuffer.add(createMessageFromJSON(messageJsonObject));
//        //         this.buffer.put(messageSource, sourceBuffer);
//        //     }
//        // }
//        // Message received is the expected message from the other server so process it
//        // else
//        // {
//            updateLamportClock(messageJsonObject.getInt("lamport"));
//            updateACKS(messageJsonObject);
//           //Check if ack has been received by all servers for this message.
//            if(checkAllACKS() && !deliveryQueue.isEmpty())
//            {
//                //deliver message to cassandra
//                //send delivery acknowledgement to source server
//                
//                //System.out.println("Prinitng my id "+this.myID+" delivery queue "+this.deliveryQueue.peek().getMessageId()+" message source "+this.deliveryQueue.peek().getMessageSource());
//                Message m = this.deliveryQueue.remove();
//                //if(!this.deliveryQueue.isEmpty()){
//               //System.out.println("Prinitng my id after polling "+m+" "+this.myID+" delivery queue "+this.deliveryQueue.peek().getMessageId()+" message source "+this.deliveryQueue.peek().getMessageSource());
//                //}
//
//                String clientHostName = m.getClientIP().split(":")[0];
//                Integer clientPort = Integer.parseInt(m.getClientIP().split(":")[1]); 
//                InetSocketAddress isa = new InetSocketAddress(clientHostName, clientPort);
//                // ////""tem.out.println("Delivering message with message id: " + m.getMessageId() + "from server: " + this.myID + "to cassandra and ip address of client is: " + isa);
//                deliverMessage(m.getQuery().getBytes(SingleServer.DEFAULT_ENCODING), isa);}
//            }
//            //Expecting next message from sender
//            // expectedMessageId.put(messageSource, expectedMessageId.get(messageSource)+1);
//
//            //Next message from sender has been buffered.
//            // while(buffer.get(messageSource) != null && !buffer.get(messageSource).isEmpty() && buffer.get(messageSource).peek().getMessageId() == expectedMessageId.get(messageSource))
//            // {
//            //     System.out.println("checking if this gets executed");
//            //     Message m1 = buffer.get(messageSource).poll();
//            //     //If message is an ack message then process it.
//            //     if(m1.getIsAck()) {
//            //         updateLamportClock(messageJsonObject.getInt("lamport"));
//            //         updateACKS(messageJsonObject);
//            //         if(checkAllACKS())
//            //         {
//            //             //deliver message to cassandra
//            //             //send delivery acknowledgement to source server
//            //             Message m = deliveryQueue.poll();
//            //             String clientHostName = m.getClientIP().split(":")[0];
//            //             Integer clientPort = Integer.parseInt(m.getClientIP().split(":")[1]); 
//            //             InetSocketAddress isa = new InetSocketAddress(clientHostName, clientPort);
//            //             ////""tem.out.println("Delivering message with message id: " + m.getMessageId() + "from server: " + this.myID + "to cassandra and ip address of client is: " + isa);
//            //             deliverMessage(m.getQuery().getBytes(SingleServer.DEFAULT_ENCODING), isa);
//            //         }
//            //         //Expecting next message from sender
//            //         expectedMessageId.put(messageSource, expectedMessageId.get(messageSource)+1);
//            //     } else
//            //     {
//            //         //Message is a multicast message so handle it seperately.
//            //         handleReceiveMulticastFromServer(new JSONObject(m1));
//            //     }
//            // }
//    //    }
//        // ////""tem.out.println(this.myID + ":" + acksReceived);
//    	} catch(Exception e)
//    	{
//    		e.printStackTrace();
//    	}
//    }
//
//    //multicast message to all the other nodes
//    private void multicast(String m)
//    {
//        ////""tem.out.println("Entered multicast");
//        // relay to other servers
//        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs())
//            if (!node.equals(myID)){
//            try {
//                this.serverMessenger.send(node, m.getBytes(SingleServer.DEFAULT_ENCODING));
//                // System.out.println("sending m to server from "+this.myID+m+node);
//            } catch (IOException e) {
//            e.printStackTrace();
//            }}
//    }
//
//    //Create a message from JSON
//    private Message createMessageFromJSON(JSONObject messageJsonObject)
//    {
//        ////""tem.out.println("Entered createMessageFromJSON");
//        Message m = new Message();
//        try {
//        m.setMessageId(messageJsonObject.getInt("messageId"));
//        m.setIsAck(false);
//        m.setQuery(messageJsonObject.getString("query"));
//        m.setLamport(messageJsonObject.getInt("lamport"));
//        m.setMessageSource(messageJsonObject.getString("messageSource"));
//        m.setAckSource(messageJsonObject.getString("ackSource"));
//        m.setAckedMessageId(messageJsonObject.getInt("ackedMessageId"));
//        m.setClientIP(messageJsonObject.getString("clientIP"));
//        } catch(Exception e)
//        {
//        	e.printStackTrace();
//        }
//        return m;
//    }
//
//    //Update lamport clocks for received messages.
//    private void updateLamportClock(Integer messageTimeStamp)
//    {
//        ////""tem.out.println("Entered updateLamportClock");
//        synchronized(lamportClock){
//            this.lamportClock.set(Math.max(this.lamportClock.get(), messageTimeStamp) + 1);
//        }
//    }
//    
//
//    //Create an ack message.
//    private Message createAckMessageAndUpdateLocalClock(JSONObject messageJsonObject) {
//        ////""tem.out.println("Entered createAckMessageAndUpdateLocalClock");
//    	Message m = new Message();
//        try{m.setIsAck(true);
//        m.setClientIP(messageJsonObject.getString("clientIP"));
//        m.setLamport(lamportClock.incrementAndGet());
//        m.setMessageSource(messageJsonObject.getString("messageSource"));
//        m.setAckSource(this.myID);
//        m.setMessageId(messageId.incrementAndGet());
//        m.setAckedMessageId(messageJsonObject.getInt("ackedMessageId"));
//        m.setQuery(messageJsonObject.getString("query"));}
//        catch(Exception e) {
//        	e.printStackTrace();
//        }
//        return m;
//    }
//
//    //Update acks received to check for in check all acks.
//    private void  updateACKS(JSONObject obj) {
//        ////""tem.out.println("Entered updateACKS");
//        System.out.println("inside update acks at my id "+this.myID+"obj: "+ obj);
//    	try {
//            String serverMessageId = obj.getString("messageSource") + "##" + Integer.toString(obj.getInt("ackedMessageId"));
//    		String val;
//    		if(acksReceived.get(serverMessageId) != null) {
//    			val = acksReceived.get(serverMessageId) + ","+obj.getString("ackSource");
//    		}
//    		else {
//    			val = obj.getString("ackSource");
//    		}
//        	acksReceived.put(serverMessageId, val);
//            //System.out.println("printing acks received at my id "+this.myID+" "+acksReceived);
//			////""tem.out.println("Server " + this.myID + " acks recvd: " + acksReceived );
//    	}
//    	catch(Exception e) {
//    		e.printStackTrace();
//    	}}
//
//    //Check for all acks.
//    public Boolean checkAllACKS() {
//        ////""tem.out.println("Entered checkAllACKS");
//        if(!this.deliveryQueue.isEmpty()) {
//            Message front = this.deliveryQueue.peek();
//            System.out.println("checking all acks for front at my id "+this.myID+" "+front);
//            // ////""tem.out.println("checking front "+front);
//            int msgId = front.getMessageId();
//            String messageSource = front.getMessageSource();
//            String serverMessageId = messageSource + "##" + Integer.toString(msgId);
//            String ackServers;
//            if(acksReceived.get(serverMessageId) != null) {
//                ackServers = acksReceived.get(serverMessageId);
//                System.out.println("Printing servers who have acked at my id "+this.myID+"to msg id "+serverMessageId+ " "+ackServers);
//                // ////""tem.out.println("printing ackServers "+ackServers);
//                String[] serversArray = ackServers.split(",");
//                int acksSize = serversArray.length;
//                int n = this.serverMessenger.getNodeConfig().getNodeIDs().size();
//                if(acksSize == n-1) {
//                    System.out.println("at my id true"+this.myID);
//                    return true;
//                }
//            }
//        }
//        return false;
//    }
//    
//
//    //Deliver message to cassandra and send delivery acknowledgement to the initial server contacted by client for global commit semantics.
//    private void deliverMessage(byte[] bytes, InetSocketAddress isa) {
//        ////""tem.out.println("Entered deliverMessage");
//        // send request from client to cassandra 
//        try {
//            String query = "";
//            String queryId = "";
//            String resultString = "Operation Completed on cassandra by server - " + this.myID;
//            String request = new String(bytes, SingleServer.DEFAULT_ENCODING);
//            if(request.contains(":id:")) {
//            	query = request.split(":id:")[0];
//            	queryId = request.split(":id:")[1];
//            }
//            else
//            {
//            	query = request;
//            }
//            System.out.println("my server: " + this.myID + " Query: " + query);
//            session.execute(query);
//            if(!queryId.isEmpty())
//            {
//            	resultString += ":id:" + queryId;
//            }
//            this.clientMessenger.send(isa, resultString.getBytes(SingleServer.DEFAULT_ENCODING));   
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    //Close all resources.
//    public void close() {
//        super.close();
//        this.serverMessenger.stop();
//    }
//
//    public static void main(String[] args) throws IOException {
//        if(args.length>1)
//            for(int i=1; i<args.length; i++)
//                new MyDBReplicatedServer(NodeConfigUtils.getNodeConfigFromFile(args[0],
//                        SERVER_PREFIX, SERVER_PORT_OFFSET), args[i].trim(),
//                        new InetSocketAddress("localhost", 9042));
//        else log.info("Incorrect number of arguments; not starting any server");
//    }
//}
//
//
//
//
//
//
//


package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import javax.print.DocFlavor.STRING;

import org.apache.cassandra.cql3.Json;
import org.json.JSONObject;


/**
 * This class should implement your replicated database server. Refer to
 * {@link ReplicatedServer} for a starting point.
 */
public class MyDBReplicatedServer extends MyDBSingleServer {

    Comparator<Message> messageLamportComparator = new Comparator<Message>() {

        @Override
        public int compare(Message o1, Message o2) {
            // TODO Auto-generated method stub
            if(o1.getLamport() > o2.getLamport()) {
                return 1;
            } else if (o2.getLamport() > o1.getLamport()) {
                return -1;
            } else {
                return o1.getMessageSource().compareTo(o2.getMessageSource());
            }
        }
        
    };

    Comparator<Message> messageIdComparator = new Comparator<Message>() {
        @Override
        public int compare(Message o1, Message o2) {
            // TODO Auto-generated method stub
            return o1.getMessageId() - o2.getMessageId(); 
        }
    };

    public static final String SERVER_PREFIX = "server.";
    public static final int SERVER_PORT_OFFSET = 1000;

    protected final String myID;
    protected final MessageNIOTransport<String,String> serverMessenger;

    //This server's delivery queue which is Lamport clock ordered.
    protected PriorityBlockingQueue<Message> deliveryQueue = new PriorityBlockingQueue<Message>(5,messageLamportComparator);
    
    //This server is storing all the messages that have not arrived in order for the server mentioned in the key of this map.
    protected ConcurrentMap<String, PriorityQueue<Message>> buffer = new ConcurrentHashMap<String, PriorityQueue<Message>> ();
    
    //This server is expecting the next message ID to arrive from the server mentioned in key of this map.
    protected ConcurrentMap<String, Integer> expectedMessageId = new ConcurrentHashMap<String, Integer> ();
    
    //Lamport Clock
    public AtomicInteger lamportClock = new AtomicInteger(0);
    
    //this server's Message id tracker for messages from client
    public AtomicInteger messageId = new AtomicInteger(0);
    
    //Key: message id (server##messageId), Value: comma separated server ids
    public ConcurrentMap<String,String> acksReceived = new ConcurrentHashMap<String,String>();

    //Key: message id (server##messageId), Value: comma separated server ids
    public ConcurrentMap<String,String> deliveriesReceived = new ConcurrentHashMap<String,String>();
    
    public List<String> deliveredList = Collections.synchronizedList(new ArrayList<String>());

    //Setup
    public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
                                InetSocketAddress isaDB) throws IOException {
            super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
            nodeConfig.getNodePort(myID)-SERVER_PORT_OFFSET), isaDB, myID);
            this.myID = myID;
            this.serverMessenger = new
            MessageNIOTransport<String, String>(myID, nodeConfig,
            new AbstractBytePacketDemultiplexer() {
                        @Override
                        public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                            handleMessageFromServer(bytes, nioHeader);
                            return true;
                        }
                    }, true);
            for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
                expectedMessageId.put(node, 1);
                ////""tem.out.println("Initial expected message ids are: " + expectedMessageId);
            } 
            log.log(Level.INFO, "Server {0} started on {1}", new Object[]{this.myID, this.clientMessenger.getListeningSocketAddress()});
    }

    //Wrapper function to handle message from client
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        ////""tem.out.println("Entered handleMessageFromClient");
        try {       
            handleReceiveMessageFromClient(new String(bytes, SingleServer.DEFAULT_ENCODING),header);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    //handle message from client
    private void handleReceiveMessageFromClient(String request, NIOHeader header) 
    {
        ////""tem.out.println("Entered handleReceiveMessageFromClient");
        //Create message
        synchronized(lamportClock){
        Message m = new Message();
        m.setIsAck(false);
        m.setMessageId(messageId.incrementAndGet());
        m.setQuery(request);
        m.setMessageSource(this.myID);
        m.setAckSource(this.myID);
        m.setAckedMessageId(messageId.get());
        m.setLamport(lamportClock.incrementAndGet());

        // ////""tem.out.println("Received message from: host(" + header.sndr.getHostName() + ")" + " port(" + header.sndr.getPort() + ")");
        String clientIpString = header.sndr.getHostName() + ":" + header.sndr.getPort();
        m.setClientIP(clientIpString);

        //put message in delivery queue of this server
        this.deliveryQueue.put(m);
        
        //m.setLamport(lamportClock.incrementAndGet());

        JSONObject jsonMessage = new JSONObject(m);

        //System.out.println("Received messages from client "+jsonMessage);
        ////""tem.out.println("Message recieved from client before multicasting: " + jsonMessage.toString());

        System.out.println("source: " + this.myID + "  " + jsonMessage);
        multicast(jsonMessage.toString());
    }

    }

    // Wrapper to handle messages from other servers.
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        ////""tem.out.println("Entered handleMessageFromServer");
        try{
            String message = new String(bytes, SingleServer.DEFAULT_ENCODING);
            if(message.contains("Operation Completed on cassandra by server :") && message.contains(":id:")){
            	System.out.println("the message qweqwe isssssssss: " + message);
                String [] resultStringSplit = message.split(":id:");
                String [] resultStringSplit1 = message.split(":qid:");
                String messageDeliveryNode =  resultStringSplit[1];
                String messageSourceNode =  resultStringSplit[2];
                String messageId =  resultStringSplit[3];
                String clientHost = resultStringSplit[4];
                String clientPort = resultStringSplit[5];
                String query = resultStringSplit[6];
                String serverMessageId = messageSourceNode + "##" + messageId;
                String val;
                if(deliveriesReceived.get(serverMessageId) != null) {
                    val = deliveriesReceived.get(serverMessageId) + ","+messageDeliveryNode;
                }
                else {
                    val = messageDeliveryNode;
                }
                deliveriesReceived.put(serverMessageId, val);

                String deliveryServers = deliveriesReceived.get(serverMessageId);
                //System.out.println("Printing servers who have acked at my id "+this.myID+"to msg id "+serverMessageId+ " "+ackServers);
                // ////""tem.out.println("printing ackServers "+ackServers);
                String[] serversArray = deliveryServers.split(",");
                int acksSize = serversArray.length;
                int n = this.serverMessenger.getNodeConfig().getNodeIDs().size();
                if(acksSize == n-1) {
                    //System.out.println("at my id true"+this.myID);
                    InetSocketAddress isa = new InetSocketAddress(clientHost, Integer.parseInt(clientPort));
                    if(!resultStringSplit[1].isEmpty()) {
                    	query = query + ":qid:" + resultStringSplit[1];
                    }
                    deliverMessage(query.getBytes(), messageSourceNode, isa, Integer.parseInt(messageId));
                }
            }
            else{
            	System.out.println("The message isssss: " + message);
                JSONObject messageJsonObject = new JSONObject(message);
            
                System.out.println(this.myID + " Recieved message: " + messageJsonObject);

                //received an ack
                if(messageJsonObject.getBoolean("isAck"))
                {
                    handleReceiveAckFromServer(messageJsonObject);
                }
                //Recieved a multicast message
                else  
                {
                    handleReceiveMulticastFromServer(messageJsonObject);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        log.log(Level.INFO, "{0} received relayed message from {1}",
                new Object[]{this.myID, header.sndr}); // simply log
    }


    //Handles multicast messages sent to this server by other servers
    private void handleReceiveMulticastFromServer(JSONObject messageJsonObject) 
    {
        ////""tem.out.println("Entered handleReceiveMulticastFromServer");
        synchronized(lamportClock) {
        try {
        // System.out.println(this.myID+ " : " + messageJsonObject);

        // ////""tem.out.println("multicast recieved from server: " + messageJsonObject.toString());
        String messageSource = messageJsonObject.getString("messageSource");
        //Implementing underlying FIFO by buffering messages that arrive not in order from the other servers
        // System.out.println("printing check at my id "+this.myID+" received message id "+messageJsonObject.getInt("messageId")+" expected message id "+this.expectedMessageId.get(messageSource));
        if(messageJsonObject.getInt("messageId") != this.expectedMessageId.get(messageSource)){
        //   System.out.println("inserting in buffer at my id "+this.myID+" received message id "+messageJsonObject.getInt("messageId"));
            if(this.buffer.get(messageSource) == null ) {
                //""tem.out.println(messageIdComparator);
                PriorityQueue<Message> sourceBuffer = new PriorityQueue<Message>(5, messageIdComparator);
                sourceBuffer.add(createMessageFromJSON(messageJsonObject));
                //""tem.out.println(sourceBuffer);
                this.buffer.put(messageSource,sourceBuffer);
//                System.out.println("inside if buffer "+this.buffer);
            }
            else {
                PriorityQueue<Message> sourceBuffer = buffer.get(messageSource);
                sourceBuffer.add(createMessageFromJSON(messageJsonObject));
                this.buffer.put(messageSource, sourceBuffer);
//                System.out.println("inside else buffer "+this.buffer);

            }
        }
        //Message received is the expected message from the other server so process it
        else {
            // System.out.println("inserting into delivery queue my id "+this.myID+" received message id "+messageJsonObject.getInt("messageId"));
            updateLamportClock(messageJsonObject.getInt("lamport"));
            //Updating acks for sender in this server to check for head all acks
            updateACKS(messageJsonObject);
            // ////""tem.out.println(this.myID + ":" + acksReceived);
            Message m = createMessageFromJSON(messageJsonObject);
            // System.out.println(this.myID+ " 213: " + m);
            this.deliveryQueue.put(m);
            
            if(checkAllACKS() && !deliveryQueue.isEmpty())
            {
                //deliver message to cassandra
                //send delivery acknowledgement to source server
                
                //System.out.println("Prinitng my id "+this.myID+" delivery queue "+this.deliveryQueue.peek().getMessageId()+" message source "+this.deliveryQueue.peek().getMessageSource());
                Message m1 = this.deliveryQueue.remove();
                //if(!this.deliveryQueue.isEmpty()){
               //System.out.println("Prinitng my id after polling "+m+" "+this.myID+" delivery queue "+this.deliveryQueue.peek().getMessageId()+" message source "+this.deliveryQueue.peek().getMessageSource());
                //}

               String clientHostName = m1.getClientIP().split(":")[0];
               Integer clientPort = Integer.parseInt(m1.getClientIP().split(":")[1]); 
               InetSocketAddress isa = new InetSocketAddress(clientHostName, clientPort);
                // ////""tem.out.println("Delivering message with message id: " + m.getMessageId() + "from server: " + this.myID + "to cassandra and ip address of client is: " + isa);
                deliverMessage(m1.getQuery().getBytes(SingleServer.DEFAULT_ENCODING), m1.getMessageSource(), isa, m1.getMessageId());}
            
            Message ack = createAckMessageAndUpdateLocalClock(messageJsonObject);
            JSONObject jsonAck = new JSONObject(ack);
            // ////""tem.out.println("ack from server: " +jsonAck.toString());
            multicast(jsonAck.toString());
            //expecting the next message from the sender.
            expectedMessageId.put(messageSource, expectedMessageId.get(messageSource)+1);
            // System.out.println("printing outer delivery queue at server "+this.myID+" "+this.deliveryQueue);
            //Next message from sender has been buffered.
            while(buffer.get(messageSource) != null && !buffer.get(messageSource).isEmpty() && buffer.get(messageSource).peek().getMessageId() == expectedMessageId.get(messageSource))
            {
                Message m1 = buffer.get(messageSource).poll();
                
                //If message is a multicast message then process it.
                if(!m1.getIsAck()) {
                    JSONObject m1JsonObject = new JSONObject(m1);
                    updateLamportClock(m1JsonObject.getInt("lamport"));
                    //Updating acks for sender in this server to check for head all acks
                    updateACKS(m1JsonObject);
                    this.deliveryQueue.put(m1);
                    
                    if(checkAllACKS() && !deliveryQueue.isEmpty())
                    {
                        //deliver message to cassandra
                        //send delivery acknowledgement to source server
                        
                        //System.out.println("Prinitng my id "+this.myID+" delivery queue "+this.deliveryQueue.peek().getMessageId()+" message source "+this.deliveryQueue.peek().getMessageSource());
                        Message m2 = this.deliveryQueue.remove();
                        //if(!this.deliveryQueue.isEmpty()){
                       //System.out.println("Prinitng my id after polling "+m+" "+this.myID+" delivery queue "+this.deliveryQueue.peek().getMessageId()+" message source "+this.deliveryQueue.peek().getMessageSource());
                        //}

                       String clientHostName = m2.getClientIP().split(":")[0];
                       Integer clientPort = Integer.parseInt(m2.getClientIP().split(":")[1]); 
                       InetSocketAddress isa = new InetSocketAddress(clientHostName, clientPort);
                        // ////""tem.out.println("Delivering message with message id: " + m.getMessageId() + "from server: " + this.myID + "to cassandra and ip address of client is: " + isa);
                        deliverMessage(m2.getQuery().getBytes(SingleServer.DEFAULT_ENCODING), m2.getMessageSource(), isa, m2.getMessageId());}
                    
                    Message ackm1 = createAckMessageAndUpdateLocalClock(m1JsonObject);
                    JSONObject jsonAckm1 = new JSONObject(ackm1);
                    // ////""tem.out.println("ack from server: " +jsonAck.toString());
                    multicast(jsonAckm1.toString());
                    //Expecting next message from sender.
                    expectedMessageId.put(messageSource, expectedMessageId.get(messageSource)+1);
                    // System.out.println("printing inner delivery queue at server "+this.myID+" "+this.deliveryQueue);
                }
                else {
                    //Message is an ack message and needs to be handled separately.
                    handleReceiveAckFromServer(new JSONObject(m1));
                }
            }
        }
        } catch(Exception e)
        {
            e.printStackTrace();
        }

    //    System.out.println("printing buffer at my id "+this.myID+" "+this.buffer);
    for (Map.Entry<String, PriorityQueue<Message>> entry : buffer.entrySet()) {
    String serverId = entry.getKey();
    PriorityQueue<Message> messageQueue = entry.getValue();

    // System.out.println("Buffer at my id " + this.myID+" printing server id "+serverId);
    // System.out.println("Messages in Buffer for " + serverId + ":");

    for (Message message : messageQueue) {
        // System.out.println("butter at my id : " + this.myID+" printing message id "+message.getMessageId() + ", Lamport Timestamp: " + message.getLamport());
    }
}
        }
    }


    //Handles ack messages sent to this erver by other servers.
     private void handleReceiveAckFromServer(JSONObject messageJsonObject)
    {
        ////""tem.out.println("Entered handleReceiveAckFromServer");
        try {
            synchronized(deliveryQueue){
        // ////""tem.out.println("Ack recieved from server: " + messageJsonObject);
        String messageSource = messageJsonObject.getString("messageSource");

        //Implementing underlying FIFO by buffering messages that arrive not in order from the other servers
        System.out.println("checking acks at my id "+this.myID+" message source: "+messageJsonObject.getString("messageSource")+"ack source: "+messageJsonObject.getString("ackSource")+" acked message id "+messageJsonObject.getInt("ackedMessageId"));

        // if(messageJsonObject.getInt("messageId") != expectedMessageId.get(messageSource)){
        //     if(buffer.get(messageSource) == null ) {
        //         PriorityQueue<Message> sourceBuffer = new PriorityQueue<Message>(5, messageIdComparator);
        //         sourceBuffer.add(createMessageFromJSON(messageJsonObject));
        //         buffer.put(messageSource,sourceBuffer);
        //     }
        //     else {
        //         PriorityQueue<Message> sourceBuffer = buffer.get(messageSource);
        //         sourceBuffer.add(createMessageFromJSON(messageJsonObject));
        //         this.buffer.put(messageSource, sourceBuffer);
        //     }
        // }
        // Message received is the expected message from the other server so process it
        // else
        // {
            updateLamportClock(messageJsonObject.getInt("lamport"));
            updateACKS(messageJsonObject);
           //Check if ack has been received by all servers for this message.
            if(checkAllACKS() && !deliveryQueue.isEmpty())
            {
                //deliver message to cassandra
                //send delivery acknowledgement to source server
                
                //System.out.println("Prinitng my id "+this.myID+" delivery queue "+this.deliveryQueue.peek().getMessageId()+" message source "+this.deliveryQueue.peek().getMessageSource());
                Message m = this.deliveryQueue.remove();
                //if(!this.deliveryQueue.isEmpty()){
               //System.out.println("Prinitng my id after polling "+m+" "+this.myID+" delivery queue "+this.deliveryQueue.peek().getMessageId()+" message source "+this.deliveryQueue.peek().getMessageSource());
                //}

               String clientHostName = m.getClientIP().split(":")[0];
               Integer clientPort = Integer.parseInt(m.getClientIP().split(":")[1]); 
               InetSocketAddress isa = new InetSocketAddress(clientHostName, clientPort);
                // ////""tem.out.println("Delivering message with message id: " + m.getMessageId() + "from server: " + this.myID + "to cassandra and ip address of client is: " + isa);
                deliverMessage(m.getQuery().getBytes(SingleServer.DEFAULT_ENCODING), m.getMessageSource(), isa, m.getMessageId());}
            }
            //Expecting next message from sender
            // expectedMessageId.put(messageSource, expectedMessageId.get(messageSource)+1);

            //Next message from sender has been buffered.
            // while(buffer.get(messageSource) != null && !buffer.get(messageSource).isEmpty() && buffer.get(messageSource).peek().getMessageId() == expectedMessageId.get(messageSource))
            // {
            //     System.out.println("checking if this gets executed");
            //     Message m1 = buffer.get(messageSource).poll();
            //     //If message is an ack message then process it.
            //     if(m1.getIsAck()) {
            //         updateLamportClock(messageJsonObject.getInt("lamport"));
            //         updateACKS(messageJsonObject);
            //         if(checkAllACKS())
            //         {
            //             //deliver message to cassandra
            //             //send delivery acknowledgement to source server
            //             Message m = deliveryQueue.poll();
            //             String clientHostName = m.getClientIP().split(":")[0];
            //             Integer clientPort = Integer.parseInt(m.getClientIP().split(":")[1]); 
            //             InetSocketAddress isa = new InetSocketAddress(clientHostName, clientPort);
            //             ////""tem.out.println("Delivering message with message id: " + m.getMessageId() + "from server: " + this.myID + "to cassandra and ip address of client is: " + isa);
            //             deliverMessage(m.getQuery().getBytes(SingleServer.DEFAULT_ENCODING), isa);
            //         }
            //         //Expecting next message from sender
            //         expectedMessageId.put(messageSource, expectedMessageId.get(messageSource)+1);
            //     } else
            //     {
            //         //Message is a multicast message so handle it seperately.
            //         handleReceiveMulticastFromServer(new JSONObject(m1));
            //     }
            // }
    //    }
        // ////""tem.out.println(this.myID + ":" + acksReceived);
        } catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    //multicast message to all the other nodes
    private void multicast(String m)
    {
        ////""tem.out.println("Entered multicast");
        // relay to other servers
        for (String node : this.serverMessenger.getNodeConfig().getNodeIDs())
            if (!node.equals(myID)){
            try {
                this.serverMessenger.send(node, m.getBytes(SingleServer.DEFAULT_ENCODING));
                // System.out.println("sending m to server from "+this.myID+m+node);
            } catch (IOException e) {
            e.printStackTrace();
            }}
    }

    //Create a message from JSON
    private Message createMessageFromJSON(JSONObject messageJsonObject)
    {
        ////""tem.out.println("Entered createMessageFromJSON");
        Message m = new Message();
        try {
        m.setMessageId(messageJsonObject.getInt("messageId"));
        m.setIsAck(false);
        m.setQuery(messageJsonObject.getString("query"));
        m.setLamport(messageJsonObject.getInt("lamport"));
        m.setMessageSource(messageJsonObject.getString("messageSource"));
        m.setAckSource(messageJsonObject.getString("ackSource"));
        m.setAckedMessageId(messageJsonObject.getInt("ackedMessageId"));
        m.setClientIP(messageJsonObject.getString("clientIP"));
        } catch(Exception e)
        {
            e.printStackTrace();
        }
        return m;
    }

    //Update lamport clocks for received messages.
    private void updateLamportClock(Integer messageTimeStamp)
    {
        ////""tem.out.println("Entered updateLamportClock");
        synchronized(lamportClock){
            this.lamportClock.set(Math.max(this.lamportClock.get(), messageTimeStamp) + 1);
        }
    }
    

    //Create an ack message.
    private Message createAckMessageAndUpdateLocalClock(JSONObject messageJsonObject) {
        ////""tem.out.println("Entered createAckMessageAndUpdateLocalClock");
        Message m = new Message();
        try{m.setIsAck(true);
        m.setClientIP(messageJsonObject.getString("clientIP"));
        m.setLamport(lamportClock.incrementAndGet());
        m.setMessageSource(messageJsonObject.getString("messageSource"));
        m.setAckSource(this.myID);
        m.setMessageId(messageId.get());
        m.setAckedMessageId(messageJsonObject.getInt("ackedMessageId"));
        m.setQuery(messageJsonObject.getString("query"));}
        catch(Exception e) {
            e.printStackTrace();
        }
        return m;
    }

    //Update acks received to check for in check all acks.
    private void  updateACKS(JSONObject obj) {
        ////""tem.out.println("Entered updateACKS");
        System.out.println("inside update acks at my id "+this.myID+"obj: "+ obj);
        try {
            String serverMessageId = obj.getString("messageSource") + "##" + Integer.toString(obj.getInt("ackedMessageId"));
            String val;
            if(acksReceived.get(serverMessageId) != null) {
                val = acksReceived.get(serverMessageId) + ","+obj.getString("ackSource");
            }
            else {
                val = obj.getString("ackSource");
            }
            acksReceived.put(serverMessageId, val);
            //System.out.println("printing acks received at my id "+this.myID+" "+acksReceived);
            ////""tem.out.println("Server " + this.myID + " acks recvd: " + acksReceived );
        }
        catch(Exception e) {
            e.printStackTrace();
        }}

    //Check for all acks.
    public Boolean checkAllACKS() {
        ////""tem.out.println("Entered checkAllACKS");
        if(!this.deliveryQueue.isEmpty()) {
            Message front = this.deliveryQueue.peek();
            System.out.println("checking all acks for front at my id "+this.myID+" "+front);
            // ////""tem.out.println("checking front "+front);
            int msgId = front.getMessageId();
            String messageSource = front.getMessageSource();
            String serverMessageId = messageSource + "##" + Integer.toString(msgId);
            String ackServers;
            if(acksReceived.get(serverMessageId) != null) {
                ackServers = acksReceived.get(serverMessageId);
                System.out.println("Printing servers who have acked at my id "+this.myID+"to msg id "+serverMessageId+ " "+ackServers);
                // ////""tem.out.println("printing ackServers "+ackServers);
                String[] serversArray = ackServers.split(",");
                int acksSize = serversArray.length;
                int n = this.serverMessenger.getNodeConfig().getNodeIDs().size();
                if(acksSize == n-1) {
                    System.out.println("at my id true"+this.myID);
                    return true;
                }
            }
        }
        return false;
    }
    

    //Deliver message to cassandra and send delivery acknowledgement to the initial server contacted by client for global commit semantics.
    private void deliverMessage(byte[] bytes, String messageSource, InetSocketAddress isa, Integer messageId) {
        ////""tem.out.println("Entered deliverMessage");
        // send request from client to cassandra 
        try {
        	synchronized(deliveredList) {
        	if(messageSource.equals(this.myID)) {
        		String query ="";
            	String queryId = "";
            	String resultString = "Operation Completed on cassandra by server :" + this.myID;
              String request = new String(bytes, SingleServer.DEFAULT_ENCODING);
              System.out.println("Queryyyyyyy: " + request);
              if(request.contains(":qid:")) {
              	query = request.split(":qid:")[0];
              	queryId = request.split(":qid:")[1];
              }
              else if(query.contains(":id:"))
              {
              	query = request.split(":id:")[0];
              	queryId = request.split(":id:")[1];
              }
              else
              {
            	  System.out.println("Here at 1103: request iss:" + request);
            	  query =request;
              }
              System.out.println("1100my server: " + this.myID + " Query: " + query);
              if(!deliveredList.contains(messageSource + "##" + messageId)) {
              	session.execute(query.split(":id:")[0]);
              	deliveredList.add(messageSource + "##" + messageId);
              	if(!queryId.isEmpty())
                {
                	resultString += ":id1:" + query + ":id1:" + queryId;
                }
                  this.clientMessenger.send(isa, resultString.getBytes(SingleServer.DEFAULT_ENCODING));
              }
                }
            else{
                String queryId = "";
                String query = "";
                String request = new String(bytes, SingleServer.DEFAULT_ENCODING);
                String resultString = "Operation Completed on cassandra by server :id:" + this.myID + ":id:" + messageSource + ":id:" + messageId + ":id:" + isa.getHostName() + ":id:" + isa.getPort();
                if(request.contains(":id:")) {
                    query = request.split(":id:")[0];
                    queryId = request.split(":id:")[1];
                }
                else
                {
                    query = request;
                }
                System.out.println("1125my server: " + this.myID + " Query: " + query);
                if(!deliveredList.contains(messageSource + "##" + messageId)) {
                	session.execute(query);
                	deliveredList.add(messageSource + "##" + messageId);
                }
                if(!queryId.isEmpty())
                {
                    resultString += ":id:" + query + ":qid:" + queryId;
                }
                else
                {
                	resultString += ":id:" + query;
                }
                
                //return resultString.getBytes(SingleServer.DEFAULT_ENCODING);
                System.out.println("result string is at 1139:   " + resultString);
                this.serverMessenger.send(messageSource, resultString.getBytes(SingleServer.DEFAULT_ENCODING));
            }   
        } 
        }catch (IOException e) {
            e.printStackTrace();
        }
    }


    //Close all resources.
    public void close() {
        super.close();
        this.serverMessenger.stop();
    }

    public static void main(String[] args) throws IOException {
        if(args.length>1)
            for(int i=1; i<args.length; i++)
                new MyDBReplicatedServer(NodeConfigUtils.getNodeConfigFromFile(args[0],
                        SERVER_PREFIX, SERVER_PORT_OFFSET), args[i].trim(),
                        new InetSocketAddress("localhost", 9042));
        else log.info("Incorrect number of arguments; not starting any server");
    }
}
