package io.openmessaging.demo;

import io.openmessaging.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by lee on 5/16/17.
 */
public class DefaultProducer implements Producer {
    private DefaultMessageFactory messageFactory = new DefaultMessageFactory();
    private KeyValue properties;

    private MessageWriter messageWriter;
   // private MessageStore messageStore;



    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
       // messageStore = MessageStore.getInstance(properties);
        //String storePath = properties.getString("STORE_PATH");
        messageWriter = new MessageWriter(properties);
        new Thread(messageWriter).start();
    }
    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }
    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }



    @Override public void start() {}

    @Override public void shutdown() {}

    @Override public KeyValue properties() { return properties; }



    @Override public   void send(Message message) {
        messageWriter.addMessage(message);

        /*
        String producerId = Thread.currentThread().getName();
        String queueOrTopic = null;
        if (message.headers().containsKey(MessageHeader.QUEUE))
            queueOrTopic = message.headers().getString(MessageHeader.QUEUE);
        else queueOrTopic = message.headers().getString(MessageHeader.TOPIC);

        queueOrTopic = queueOrTopic.substring(1);
        /*
        synchronized (messageStore) {
            if (!messageStore.bucketTable.containsKey(queueOrTopic))
                messageStore.bucketTable.put(queueOrTopic, new HashSet<>());
            Set<String> producerSet = messageStore.bucketTable.get(queueOrTopic);
            if (!producerSet.contains(producerId))
                producerSet.add(producerId);
        }
        */

        /*
        if (!messageStore.bucketTable.containsKey(queueOrTopic))
            messageStore.bucketTable.put(queueOrTopic, new HashSet<>());
        Set<String> producerSet = messageStore.bucketTable.get(queueOrTopic);
        if (!producerSet.contains(producerId))
            producerSet.add(producerId);
         */



    }

    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message) {
        throw new  UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    public void flush() {
        Message FIN = messageFactory.createBytesMessageToQueue("", "".getBytes());
        send(FIN);
        //messageStore.writeIndexFile();

    }
}
