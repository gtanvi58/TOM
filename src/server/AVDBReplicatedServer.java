package server;

import client.AVDBClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

/**
 * This class should implement your replicated database server. Refer to
 * {@link ReplicatedServer} for a starting point.
 */
public class AVDBReplicatedServer extends SingleServer {
	final private Session session;
	final private Cluster cluster;
	protected final String myID;
	protected final MessageNIOTransport<String, String> serverMessenger;

	protected String leader;
	// this is the message queue used to track which messages have not been
	// sent yet
	private ConcurrentHashMap<Long, JSONObject> queue =
			new ConcurrentHashMap<Long, JSONObject>();
	// array used to track servers that have ACK'ed request execution
	private CopyOnWriteArrayList<String> notAcked;

	// the sequencer to track the most recent request in the queue
	private long requestNumber = 0;

	synchronized Long incrReqNum() {
		return requestNumber++;
	}

	// the sequencer to track the next request to be sent
	private long expectedRequestNumber = 0;

	synchronized Long incrExpected() {
		return expectedRequestNumber++;
	}

	protected enum Type {
		TYPE, // REQUEST, PROPOSAL, ACK, or COMMIT
		PROPOSAL, // leader broadcasts PROPOSAL to all  nodes
		ACK, // all nodes send back ACK to leader
		COMMIT, // leader sends COMMIT message to entry server
		ENTRY_SERVER, // ID of server that received client request
		CLIENT_SADDR, // client socket address to send back COMMIT
		IS_JSON, // indicates if request came as JSON from client
		SEQNUM, // request number used by servers (different from clients)
	}

	/**
	 * @param nodeConfig
	 * @param myID
	 * @param isaDB
	 * @throws IOException
	 */
	public AVDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
								InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);
		session = (cluster =
				Cluster.builder().addContactPoint("127.0.0.1").build()).connect(myID);
		log.log(Level.INFO, "Server {0} added cluster contact point",
				new Object[]{myID,});
		// leader is always the first node in the nodeConfig
		for (String node : nodeConfig.getNodeIDs()) {
			this.leader = node;
			break;
		}
		this.myID = myID;
		this.serverMessenger = new MessageNIOTransport<String, String>(myID,
				nodeConfig, new AbstractBytePacketDemultiplexer() {
			@Override
			public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
				handleMessageFromServer(bytes, nioHeader);
				return true;
			}
		}, true);
		log.log(Level.INFO, "Server {0} started on {1}",
				new Object[]{this.myID,
						this.clientMessenger.getListeningSocketAddress()});
	}

	// Entry server simply relays client request to leader.
	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {

		System.out.println("handle message from client");
		String request = new String(bytes);
		log.log(Level.INFO, "{0} received client request '{1}' from {2}",
				new Object[]{this.myID, request, header.sndr});

		// try to decode as JSON, else proceed as string
		JSONObject json = null;
		try {
			json = new JSONObject(request).put(Type.IS_JSON.toString(), true);
			request = json.getString(AVDBClient.Keys.REQUEST.toString());
		} catch (JSONException e) {
			// here => request not sent via callbackSend
		}

		JSONObject packet = (json != null ? json : new JSONObject());
		// forward the request to the leader (possibly self) as a proposal
		try {
			this.serverMessenger.send(leader, packet.put(Type.TYPE.toString(),
							AVDBClient.Keys.REQUEST.toString())
					// insert request value, possibly JSON string
					.put(AVDBClient.Keys.REQUEST.toString(), request)
					// entry server
					.put(Type.ENTRY_SERVER.toString(), myID)
					// originating client
					.put(Type.CLIENT_SADDR.toString(), header.sndr).toString().getBytes());
			log.log(Level.INFO,
					"Entry server {0} relays REQUEST to leader " + "{1}: {2}",
					new Object[]{this.myID, leader, packet});
		} catch (IOException | JSONException e) {
			e.printStackTrace();
			// JSONException should never happen and IOException can
			// compromise liveness but not safety
			if (e instanceof IOException)
				log.log(Level.SEVERE,
						"Unable to send REQUEST {0} to leader " + "{1}",
						new Object[]{packet, leader});
		}
	}

	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {

		// deserialize the request, must be JSON if here
		JSONObject json = null;
		try {
			json = new JSONObject(new String(bytes));
		} catch (JSONException e) {
			e.printStackTrace(); // must never happen
			log.log(Level.SEVERE, "{0} unexpectedly received relayed non" +
					"-JSON message from server {1}: {2}",
					new Object[]{this.myID, header.sndr, new String(bytes)});
			// no point proceeding further
			return;
		}
        try{
log.log(Level.INFO, (myID.equals(leader) ? "Leader" : "Follower") + " "
				+ "{0} received {1} message: {2}    from {3}",
				new Object[]{this.myID, json.getString(Type.TYPE.toString()),
						json, header.sndr});
        }
        catch(Exception e){
            e.printStackTrace();
        }
		

		// message-type-specific processing logic
		try {
			String type = json.getString(Type.TYPE.toString());

			/////////////// REQUEST relayed by entry server
			if (type.equals(AVDBClient.Keys.REQUEST.toString())) {
				if (myID.equals(leader)) {

					// put the request into the queue
					Long reqId = incrReqNum();
					json.put(Type.SEQNUM.toString(), reqId);
					queue.put(reqId, json);
					log.log(Level.INFO, "Leader {0} enqueued PROPOSAL {1}",
							new Object[]{this.myID, json});
					this.broadcastProposalIfReady();
				}
				else
					log.log(Level.SEVERE,
							"{0} received REQUEST message from " + "{1} which "
									+ "should not be possible.",
							new Object[]{this.myID, header.sndr});
			} /////////////// end of REQUEST handling

			/////////////// PROPOSAL broadcast by leader
			else if (type.equals(Type.PROPOSAL.toString())) {

				// execute the query and send back the ACK
				String query =
						json.getString(AVDBClient.Keys.REQUEST.toString());
				session.execute(query);
				JSONObject responseToLeader = json.put(Type.TYPE.toString(),
								Type.ACK.toString())
						// insert self ID as ACK  in response field
						.put(AVDBClient.Keys.RESPONSE.toString(), this.myID);
				serverMessenger.send(header.sndr,
						responseToLeader.toString().getBytes());
			} /////////////// end of PROPOSAL handling

			/////////////// ACK by followers to leader after executing request
			else if (type.equals(Type.ACK.toString())) {
				// only the leader needs to handle ACK
				if (myID.equals(leader)) {
					String node =
							json.getString(AVDBClient.Keys.RESPONSE.toString());
					if (markCurrentRequestAsAckedFrom(node)) {
						//if leader received all acks, send commit to entry
						// server, but only if JSON client request
						if (json.has(Type.IS_JSON.toString()) && json.getBoolean(Type.IS_JSON.toString()))
							serverMessenger.send(
									// construct entry server address
									new InetSocketAddress(this.serverMessenger.getNodeConfig().getNodeAddress(json.getString(Type.ENTRY_SERVER.toString())), this.serverMessenger.getNodeConfig().getNodePort(json.getString(Type.ENTRY_SERVER.toString()))),
									// convey committed semantics
									json.put(Type.TYPE.toString(),
											Type.COMMIT.toString()).toString().getBytes());
						// prepare to send the next request
						this.incrExpected();
						this.broadcastProposalIfReady();
					}
				}
				// else unrecognized message type
				else
					log.log(Level.SEVERE,
							"{0} received unexpected " + "ACK " + "message " + "from {1}.", new Object[]{this.myID, header.sndr});
			} /////////////// end of ACK handling

			/////////////// commit message from leader received by entry server
			else if (type.equals(Type.COMMIT.toString())) {
				// convey committed semantics to client
				this.clientMessenger.send(getSocketAddress(new String[]{json.getString(Type.CLIENT_SADDR.toString()).replace("/", "")}),
						// change type to response message
						json.put(Type.TYPE.toString(),
								AVDBClient.Keys.RESPONSE.toString()).toString().getBytes(SingleServer.DEFAULT_ENCODING));
				log.log(Level.INFO,
						"Entry server {0} sends commit to client:" + " {1}",
						new Object[]{myID, json});
			} /////////////// end of COMMIT handling

			// unexpected message, shouldn't be here
			else
				log.log(Level.SEVERE, "{0} received unrecognized message from "
						+ "{1} which should not possible.",
						new Object[]{this.myID, header.sndr});
		} catch (JSONException | IOException e) {
			// JSONException should never occur and IOException can
			// compromise liveness but not safety
			e.printStackTrace();
			if (e instanceof IOException)
				log.log(Level.WARNING,
						"Unable to send processing outcome of " + "{0} ",
						new Object[]{json});
		}
	}

	private boolean isReadyToSend(long expectedId) {
		if (queue.size() > 0 && queue.containsKey(expectedId)) return true;
		return false;
	}

	private void broadcastProposalIfReady() {
		JSONObject proposal = null;
		synchronized (this) {
			if (isReadyToSend(expectedRequestNumber))
				proposal = queue.remove(expectedRequestNumber);
		}
		if (proposal != null) {
			refreshCurrentRequestAckTracker(); // no synchronization needed
            try{
broadcastRequest(proposal.put(Type.TYPE.toString(),
					Type.PROPOSAL.toString()));
            }
            catch(Exception e){
                e.printStackTrace();
            }
			
		}
	}

	private void broadcastRequest(JSONObject req) {
		for (String node : this.serverMessenger.getNodeConfig().getNodeIDs())
			try {
				this.serverMessenger.send(node, req.toString().getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		log.log(Level.INFO, "Leader {0} broadcast PROPOSAL {1}",
				new Object[]{myID, req});
	}

	private void refreshCurrentRequestAckTracker() {
		notAcked = new CopyOnWriteArrayList<String>();
		for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
			notAcked.add(node);
		}
	}

	private boolean markCurrentRequestAsAckedFrom(String node) {
		if (!notAcked.remove(node))
			log.log(Level.SEVERE, "Leader does not have the key {0} in " +
					"its notAcked", new Object[]{node});
		if (notAcked.size() == 0) return true;
		return false;
	}

	@Override
	public void close() {
		super.close();
		this.serverMessenger.stop();
		session.close();
		cluster.close();
	}

	/**
	 * @param args args[0] must be server.properties file and args[1] must be
	 *             myID. The server prefix in the properties file must be
	 *             ReplicatedServer.SERVER_PREFIX.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		if(args.length>1)
            for(int i=1; i<args.length; i++)
                new AVDBReplicatedServer(NodeConfigUtils.getNodeConfigFromFile(args[0],
                		ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET), args[i].trim(),
                        new InetSocketAddress("localhost", 9042));
        else log.info("Incorrect number of arguments; not starting any server");
	}
}
