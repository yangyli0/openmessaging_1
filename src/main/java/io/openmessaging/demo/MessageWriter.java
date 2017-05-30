package io.openmessaging.demo;

import io.openmessaging.*;



import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.ByteBuffer;
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
    private final int BUFFER_SIZE =  256 * 1024 * 1024;
    private final int MQ_CAPACITY = 10000;
    private String producerId;
    private BlockingQueue<Message> mq;
    private ByteBuffer indexBuf;
    private Map<String, FileChannel> indexTable;
    private long fileCursor = 0;
    private MappedByteBuffer messageBuf;
    private FileChannel messageChannel;
    private String STORE_PATH;
    private long seat = 0;

    public MessageWriter(String store_path) {
        this.STORE_PATH = store_path;
        mq = new LinkedBlockingQueue<>(MQ_CAPACITY);
        indexTable = new HashMap<>();
        indexBuf = ByteBuffer.allocate(130);
    }
    public void run() {
        producerId = Thread.currentThread().getName();
        String msgFilePath = STORE_PATH + "/" + producerId;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(msgFilePath, "rw");
            messageChannel = raf.getChannel();
            messageBuf = messageChannel.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                DefaultBytesMessage message = (DefaultBytesMessage)mq.take();
                if (new String(message.getBody()).equals(""))   break;
                KeyValue property = message.properties();
                KeyValue header = message.headers();
                String queueOrTopic;
                if (header.containsKey(MessageHeader.QUEUE))
                    queueOrTopic = header.getString(MessageHeader.QUEUE);
                else
                    queueOrTopic = header.getString(MessageHeader.TOPIC);

                queueOrTopic = queueOrTopic.substring(1);
                if (!indexTable.containsKey(queueOrTopic))
                    createIndexFile(queueOrTopic);

                byte[] propertyBytes = getKvsBytes(property);
                byte[] headerBytes = getKvsBytes(header);
                byte[] body = message.getBody();
                fill(propertyBytes, 'p');
                fill(headerBytes, 'h');
                fill(body, 'b');

                // add index
                long start = seat;
                int offset = propertyBytes.length + headerBytes.length + body.length;
                addIndex(queueOrTopic, start, offset);
                seat += offset + 1; // 跳过'\n'


            } catch (InterruptedException e) { e.printStackTrace();}
        }
    }

    public void fill(byte[] component, char flag) {
        if (flag == 'p' || flag == 'h') {
            if (messageBuf.position() + component.length > BUFFER_SIZE) {
                int k = BUFFER_SIZE - messageBuf.position();
                messageBuf.put(component, 0, k);
                try {
                    fileCursor += BUFFER_SIZE;
                    messageBuf = messageChannel.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                } catch (IOException e) { e.printStackTrace();}
                messageBuf.put(component, k, component.length - k);
            }
            else
                messageBuf.put(component);
        }
        else {
            if (messageBuf.position() + component.length + 1 > BUFFER_SIZE) {
                int k = BUFFER_SIZE - messageBuf.position();
                messageBuf.put(component, 0, k);
                try {
                    fileCursor += BUFFER_SIZE;
                    messageBuf = messageChannel.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                } catch (IOException e) { e.printStackTrace();}
                if (k < component.length)
                    messageBuf.put(component, k, component.length - k);
                messageBuf.put((byte)('\n'));
            }
            else {
                messageBuf.put(component);
                messageBuf.put((byte)('\n'));
            }
        }
    }

    public void createIndexFile(String bucket) {
        String absPath = STORE_PATH+"/" + producerId + "_" + bucket;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(absPath, "rw");
            FileChannel fc = raf.getChannel();
            indexTable.put(bucket, fc);
        } catch (IOException e) { e.printStackTrace();}
    }

    public byte[] getKvsBytes(KeyValue kvs) {
        return ((DefaultKeyValue)kvs).getBytes();
    }

    public void addIndex(String bucket, long start, int offset) {
        /*
        byte[] startBytes = Long.toString(start).getBytes();
        byte[] endBytes = Long.toString(start+offset).getBytes();
        indexBuf.put(startBytes);
        indexBuf.put((byte)('#'));
        indexBuf.put(endBytes);
        indexBuf.put((byte)('\n'));
        indexBuf.flip();
        */

        indexBuf.putLong(start);
        indexBuf.put((byte)('#'));
        indexBuf.putLong(start + offset);
        indexBuf.put((byte)('\n'));
        indexBuf.flip();

        FileChannel fc = indexTable.get(bucket);
        try {
            fc.write(indexBuf);
        } catch (IOException e) { e.printStackTrace();}
        indexBuf.clear();
    }

    public void addMessage(Message message) {
        try {
            mq.put(message);
        } catch (InterruptedException e) { e.printStackTrace();}
    }


}

