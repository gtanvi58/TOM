package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;


import com.datastax.driver.core.*;

/**
 * This class should implement the logic necessary to perform the requested
 * operation on the database and return the response back to the client.
 */
public class MyDBSingleServer extends SingleServer {

    Cluster cluster;
    Session session;

    public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
                            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);
        cluster = Cluster.builder().addContactPointsWithPorts(isaDB).build();
        session = cluster.connect(keyspace);
    }

    @Override
    // TODO: process bytes received from clients here
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // send request from client to cassandra 
        try {
            log.log(Level.INFO, "{0} received message from {1}", new Object[]
                    {this.clientMessenger.getListeningSocketAddress(), header.sndr});
            String query = "";
            String queryId = "";
            String resultString = "Operation Completed on cassandra";
            String request = new String(bytes, SingleServer.DEFAULT_ENCODING);
            if(request.contains(":id:")) {
            	query = request.split(":id:")[0];
            	queryId = request.split(":id:")[1];
            }
            else
            {
            	query = request;
            }
            // System.out.println("printing query "+query+" id:" + queryId);
            session.execute(query);
            if(!queryId.isEmpty())
            {
            	resultString += ":id:" + queryId;
            }
            // System.out.println("printing result "+resultString+" id:" + queryId);
            this.clientMessenger.send(header.sndr, resultString.getBytes(SingleServer.DEFAULT_ENCODING));   
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**public static void main(String[] args) throws IOException {
        new MyDBSingleServer(getSocketAddress(args), new InetSocketAddress
                ("localhost", 9042), "demo");
    };**/

    public void close() {
        super.close();
        cluster.close();
        // TODO: cleanly close anything you created here.
    }
}