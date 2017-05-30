package io.openmessaging.demo;

import com.sun.org.apache.bcel.internal.generic.INSTANCEOF;
import io.openmessaging.Message;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by lee on 5/29/17.
 */
public class MessageStore {

     Map<String , FileChannel> fcTable = null;
     Map<String, MappedByteBuffer> mapBufTable = null;
     Map<String, Long> curSorTable = null;
     private final int BUFFER_SIZE  =  1024 * 1024;
     private static volatile  MessageStore INSTANCE = null;


    private MessageStore() {
        fcTable = new HashMap<>();
        mapBufTable = new HashMap<>();
        curSorTable = new HashMap<>();
    }

    /*
    private static class LazyHolder {
        static final MessageStore INSTANCE = new MessageStore();
    }
    */
    public static MessageStore getInstance() {
        if (INSTANCE == null) {
            synchronized (MessageStore.class) {
                if (INSTANCE == null) {
                    INSTANCE = new MessageStore();
                }
            }
        }
        return INSTANCE;
    }


    public synchronized void insertFile(String storePath, String bucket) {
         /*
        if (!fcTable.containsKey(bucket)) {
            synchronized (this) {
                if (!fcTable.containsKey(bucket)) {
                    String absPath = storePath + "/" + bucket;
                    RandomAccessFile raf = null;
                    try {
                        raf = new RandomAccessFile(absPath, "rw");
                        FileChannel fc = raf.getChannel();
                        MappedByteBuffer mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);
                        fcTable.put(bucket,fc);
                        mapBufTable.put(bucket, mapBuf);
                        curSorTable.put(bucket, 0L);
                    } catch (IOException e) { e.printStackTrace();}
                }
            }
        }
        */
         if (!fcTable.containsKey(bucket)) {
             String absPath = storePath + "/" + bucket;
             RandomAccessFile raf = null;
             try {
                 raf = new RandomAccessFile(absPath, "rw");
                 FileChannel fc = raf.getChannel();
                 MappedByteBuffer mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);
                 fcTable.put(bucket,fc);
                 mapBufTable.put(bucket, mapBuf);
                 curSorTable.put(bucket, 0L);
             } catch (IOException e) { e.printStackTrace();}
         }
        /*
        synchronized (this) {
            if (!fcTable.containsKey(bucket)) {
                String absPath = storePath + "/" + bucket;
                RandomAccessFile raf = null;
                try {
                    raf = new RandomAccessFile(absPath, "rw");
                    FileChannel fc = raf.getChannel();
                    MappedByteBuffer mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);
                    fcTable.put(bucket,fc);
                    mapBufTable.put(bucket, mapBuf);
                    curSorTable.put(bucket, 0L);
                } catch (IOException e) { e.printStackTrace();}
            }
        }
        */
        /*
        String absPath = storePath + "/" + bucket;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(absPath, "rw");
            FileChannel fc = raf.getChannel();
            MappedByteBuffer mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);
            fcTable.put(bucket,fc);
            mapBufTable.put(bucket, mapBuf);
            curSorTable.put(bucket, 0L);
        } catch (IOException e) { e.printStackTrace();}
        */
    }

    /*
    public synchronized  void fill(byte[] component, String bucket, String name) {
        FileChannel fc = fcTable.get(bucket);
        MappedByteBuffer mapBuf = mapBufTable.get(bucket);
        Long fileCursor = curSorTable.get(bucket);
        if (name.equals("property") || name.equals("header")) {
            if (mapBuf.position() + component.length > BUFFER_SIZE) {
                // 放入第一部分
                int k = BUFFER_SIZE - mapBuf.position();
                mapBuf.put(component, 0, k);
                try {
                    fileCursor += BUFFER_SIZE;
                    mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                } catch (IOException e) { e.printStackTrace(); }

                mapBuf.put(component, k, component.length - k);
            }
            else {
                mapBuf.put(component);
            }
        }
        else {
            if (mapBuf.position() + component.length + 1 > BUFFER_SIZE) {
                int k = BUFFER_SIZE - mapBuf.position();
                mapBuf.put(component, 0, k);
                try {
                    fileCursor += BUFFER_SIZE;
                    mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                } catch (IOException e) { e.printStackTrace();}
                if (k < component.length)
                    mapBuf.put(component, k, component.length - k);
                mapBuf.put((byte)('\n'));   //插入分隔符
            }
            else {
                mapBuf.put(component);
                mapBuf.put((byte)('\n'));
            }
        }
    }
    */






}
