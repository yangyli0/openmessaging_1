package io.openmessaging.demo;

import io.openmessaging.*;


import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by lee on 5/16/17.
 */


public class MessageWriter implements Runnable {
    private KeyValue properties;

    private final int MQ_CAPACITY = 1000;
    private BlockingQueue<Message> mq;

    private final int BUFFER_SIZE = 512 * 1024 * 1024;  // TODO: 尝试256
    private FileChannel fc;
    private MappedByteBuffer mapBuf;
    private String producerId;

    private Map<String, Integer> slotSeat;    //记录当前消息的位置和长度
    private Map<String, MetaInfo> firstMsgMeta; //记录第一条消息的位置和长度，　落盘用

    private int msgLen; // 记录当前消息的长度


    public MessageWriter(KeyValue properties) {
        this.properties = properties;
        mq = new LinkedBlockingQueue<>(MQ_CAPACITY);
        slotSeat = new HashMap<>();
        firstMsgMeta = new HashMap<>();
    }

    public void run() {
        producerId = Thread.currentThread().getName();
        String absPath = properties.getString("STORE_PATH") + "/" + producerId;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(absPath, "rw");
            fc = raf.getChannel();
            mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);    // TODO:保证只映射一次
        } catch (IOException e) { e.printStackTrace();}

        while (true) {
            try {
                DefaultBytesMessage message = (DefaultBytesMessage) mq.take();
                if (new String(message.getBody()).equals(""))   break;

                KeyValue property = message.properties();
                KeyValue header = message.headers();

                // 写消息
                byte[] propertyBytes = getKvsBytes(property);
                byte[] headerBytes =getKvsBytes(header);
                byte[] body = message.getBody();
                msgLen = propertyBytes.length + headerBytes.length + body.length;

                fill(propertyBytes, 'p');
                fill(headerBytes, 'h');
                fill(body, 'b');


                String queueOrTopic;
                if (header.containsKey(MessageHeader.TOPIC))
                    queueOrTopic = header.getString(MessageHeader.TOPIC);
                else
                    queueOrTopic = header.getString(MessageHeader.QUEUE);

                queueOrTopic = queueOrTopic.substring(1);

                if (!slotSeat.containsKey(queueOrTopic)) {  // 关于该topic的第一条消息
                    int cursor = mapBuf.position();
                    slotSeat.put(queueOrTopic, cursor-8);   // 指向槽的起始位置

                    MetaInfo metaInfo = new MetaInfo(cursor-msgLen-8, msgLen);
                    firstMsgMeta.put(queueOrTopic, metaInfo);
                }

                else {
                    int slot = slotSeat.get(queueOrTopic);
                    int cursor = mapBuf.position(); // 写过消息(包括两个空位)的后位置,
                    // 填槽
                    mapBuf.position(slot);
                    mapBuf.putInt(cursor - msgLen - 8);
                    mapBuf.putInt(msgLen);
                    // 将指针恢复到写完消息后的位置
                    mapBuf.position(cursor);
                    // 更新槽位置
                    slotSeat.put(queueOrTopic, cursor-8);   // 下一个待填槽的位置
                }

            } catch (InterruptedException e) { e.printStackTrace();}
        }

        // 将firstMsgSeat写盘
        String seatFilePath = properties.getString("STORE_PATH") + "/" + "seatFile-" + producerId;
        byte[] mapBytes = getMapBytes(firstMsgMeta);

        try {
            raf = new RandomAccessFile(seatFilePath, "rw");
            fc = raf.getChannel();
            mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, mapBytes.length);
        } catch (IOException e) { e.printStackTrace();}
        mapBuf.put(mapBytes);
    }

    public void addMessage(Message message) {
        try {
            mq.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void fill(byte[] component, char flag) {
        mapBuf.put(component);
        if (flag == 'b') {
            mapBuf.putInt(-1);  // 添加占位符
            mapBuf.putInt(-1);
        }
    }

    public byte[] getKvsBytes(KeyValue kvs) {
        return ((DefaultKeyValue)kvs).getBytes();
    }

    public byte[] getMapBytes(Map<String, MetaInfo> map) {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<String, MetaInfo> entry : map.entrySet()) {
            sb.append(entry.getKey());
            sb.append(':');
            sb.append(entry.getValue().offset);
            sb.append('#');
            sb.append(entry.getValue().length);
            sb.append(',');
        }
        return sb.toString().getBytes();
    }





}























