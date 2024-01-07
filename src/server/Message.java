package server;

import java.io.Serializable;

public class Message implements Serializable {
    //Message ID of the message in the sending server
    private Integer messageId;
    //Message ID of the message for which ack message has been sent
    private Integer ackedMessageId;
    private String query;
    private Integer lamport;
    //Server to which a client sent the message
    private String messageSource;
    //Server which is sending ack message
    private String ackSource;
    private Boolean isAck;
    private String clientIP;

    public Message(){}

    public Message(String messageSource,Integer messageId) {
        this.lamport = messageId;
        this.messageSource = messageSource;
    }
    public Message(Integer messageId, Integer ackedMessageId, String query, Integer lamport, String messageSource, String ackSource, Boolean isAck, String clientIP) {
        this.messageId = messageId;
        this.query = query;
        this.lamport = lamport;
        this.messageSource = messageSource;
        this.ackSource = ackSource;
        this.isAck = isAck;
        this.clientIP = clientIP;
        this.ackedMessageId = ackedMessageId;
    }

    
    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public Integer getMessageId() {
        return messageId;
    }
    public void setMessageId(Integer messageId) {
        this.messageId = messageId;
    }

    public String getQuery() {
        return query;
    }

    public String getAckSource() {
        return ackSource;
    }

    public void setAckSource(String ackSource) {
        this.ackSource = ackSource;
    }

    public void setQuery(String query) {
        this.query = query;
    }
    public Integer getLamport() {
        return lamport;
    }
    public void setLamport(Integer lamport) {
        this.lamport = lamport;
    }
    public String getMessageSource() {
        return messageSource;
    }
    public void setMessageSource(String messageSource) {
        this.messageSource = messageSource;
    }
    
    public Boolean getIsAck() {
        return isAck;
    }
    public void setIsAck(Boolean isAck) {
        this.isAck = isAck;
    }

    public Integer getAckedMessageId() {
        return ackedMessageId;
    }

    public void setAckedMessageId(Integer ackedMessageId) {
        this.ackedMessageId = ackedMessageId;
    }

    @Override
    public String toString() {
        return "Message [messageId=" + messageId + ", ackedMessageId=" + ackedMessageId + ", query=" + query
                + ", lamport=" + lamport + ", messageSource=" + messageSource + ", ackSource=" + ackSource + ", isAck="
                + isAck + ", clientIP=" + clientIP + "]";
    }
    

}
