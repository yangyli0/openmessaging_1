package io.openmessaging.demo;

import com.sun.org.apache.bcel.internal.generic.INSTANCEOF;
import io.openmessaging.Message;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by lee on 5/29/17.
 */
public class MessageStore {
    private ConcurrentHashMap<String, List<String>> bucketTable;
    private MessageStore() {
        bucketTable = new ConcurrentHashMap<>();
    }


    private static class LazyHolder {
        static final MessageStore INSTANCE = new MessageStore();
    }

    public static MessageStore getInstance() {
        return LazyHolder.INSTANCE;
    }

    public void addProducer(String bucket, String producer) {
        if (!bucketTable.containsKey(bucket))
            bucketTable.put(bucket, new ArrayList<>());
        List<String> producerList = bucketTable.get(bucket);
        producerList.add(producer);
    }



}
