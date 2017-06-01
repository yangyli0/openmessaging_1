package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;


import java.nio.MappedByteBuffer;
import java.util.*;

/**
 * Created by lee on 5/16/17.
 */
public class DefaultPullConsumer implements PullConsumer{
    private KeyValue properties;
    private String queue;
    private List<String> bucketList = new ArrayList<>();
    private int curBucket = 0;
    private int curProducer = 0;    // producer下标

    //private Triple curTriple =  null;
    private Map<String, MessageFile> messageFileMap = null;
    private MessageBroker messageBroker;


    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        messageBroker = MessageBroker.getInstance(properties);
        messageFileMap = new HashMap<>(messageBroker.producerList.size());
    }



    @Override public KeyValue properties() { return properties; }

    @Override public  Message poll() {
        Message message = null;
        while (curBucket < bucketList.size()) {
            String bucket  = bucketList.get(curBucket);
            message = pullMessage(bucket);
            if (message != null) {
                return message;
            }
            curBucket++;
            curProducer = 0;
        }
        return null;
    }


    public Message pullMessage(String bucket) {
        Message message = null;
        List<Triple> triples = messageBroker.bucketStartInfo.get(bucket);
        while (curProducer < messageBroker.producerList.size()) {
            Triple triple = triples.get(curProducer);
            message = getMessage(triple);
            if (message != null) {
                return message;
            }
            curProducer++;
        }
        return message;
    }

    public Message getMessage(Triple triple) {

        int offset = triple.metaInfo.offset;
        int length = triple.metaInfo.length;
        if (offset == -1)   return null;
        String producer = triple.producerId;
        if (!messageFileMap.containsKey(producer))
            messageFileMap.put(producer, new MessageFile(properties, producer));
        MappedByteBuffer mapBuf = messageFileMap.get(producer).getMapBufList().get(0);  // 只映射一次

        byte[] msgBytes = new byte[length];
        mapBuf.position(offset);
        mapBuf.get(msgBytes, 0, length);

        // update triple
        triple.metaInfo.offset = mapBuf.getInt();
        triple.metaInfo.length = mapBuf.getInt();



        return assemble(msgBytes);
    }




    public Message assemble(byte[] msgBytes) {
        int i, j;
        // 获取property

        DefaultKeyValue property = new DefaultKeyValue();
        for (i = 0; i < msgBytes.length && msgBytes[i] != ','; i++);
        byte[] propertyBytes = Arrays.copyOfRange(msgBytes, 0, i);  // [start, end)
        insertKVs(propertyBytes, property);
        j = ++i; // 跳过","

        // 获取headers
        DefaultKeyValue header = new DefaultKeyValue();
        for (; i < msgBytes.length && msgBytes[i] != ','; i++);
        byte[] headerBytes = Arrays.copyOfRange(msgBytes, j, i);
        insertKVs(headerBytes, header);
        j = ++i; // 跳过","

        // 获取body
        for (; i < msgBytes.length && msgBytes[i] != '\n'; i++);
        byte[] body = Arrays.copyOfRange(msgBytes, j, i);

        String queueOrTopic = header.getString(MessageHeader.TOPIC);
        DefaultBytesMessage message = null;
        DefaultMessageFactory messageFactory = new DefaultMessageFactory();
        if (queueOrTopic != null)
            message = (DefaultBytesMessage) messageFactory.createBytesMessageToTopic(queueOrTopic, body);
        else
            message = (DefaultBytesMessage) messageFactory.createBytesMessageToQueue(queueOrTopic, body);

        message.setHeaders(header);
        message.setProperties(property);

        return message;

    }



    public void insertKVs(byte[] kvBytes, KeyValue map) {
        String kvStr = new String(kvBytes);
        String[] kvPairs = kvStr.split("\\|");
        for (String kv: kvPairs) {

            String[] tuple = kv.split("#");

            if(tuple[1].startsWith("i"))
                map.put(tuple[0], Integer.parseInt(tuple[1].substring(1)));
            else if(tuple[1].startsWith("d"))
                map.put(tuple[0], Double.parseDouble(tuple[1].substring(1)));
            else if (tuple[1].startsWith("l"))
                map.put(tuple[0], Long.parseLong(tuple[1].substring(1)));
            else
                map.put(tuple[0], tuple[1].substring(1));

        }

    }


    @Override public Message poll(KeyValue properties) { throw new UnsupportedOperationException("Unsupported"); }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName))
            throw new ClientOMSException("You have already attached to a queue: " + queue);
        queue = queueName;

        bucketList.addAll(topics);
        bucketList.add(queueName);

        // 初始化
        //messageFileMap = new ConcurrentHashMap<>(bucketList.size());
        //consumeRecord = new ConcurrentHashMap<>(bucketList.size());


    }

    @Override public void ack(String messageId) { throw new UnsupportedOperationException("Unsupported"); }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

}
