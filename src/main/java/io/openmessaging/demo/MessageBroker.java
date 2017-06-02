package io.openmessaging.demo;


import io.openmessaging.KeyValue;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lee on 5/31/17.
 */
public class MessageBroker {
    private KeyValue properties;
    private static volatile MessageBroker INSTANCE = null;
    private static int instanceCnt = 0;

    List<String> producerList = new ArrayList<>();
    File[] seatFiles;
    Map<String, List<Triple>> bucketStartInfo = null;

    public MessageBroker(KeyValue properties)  {
        this.properties = properties;
        bucketStartInfo = new HashMap<>();
        getFileSet();
        loadBucketStartInfo();

    }

    private void getFileSet() { // 获取Thread文件列表和seatFile列表
        String absPath = properties.getString("STORE_PATH");
        File dir = new File(absPath);

        seatFiles = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("seatFile-");
            }
        });

    }

    private void loadBucketStartInfo() {
        RandomAccessFile raf = null;
        for (File seatFile : seatFiles) {
            try {
                String seatFileName = seatFile.getName();
                String producerId = seatFileName.substring(9);
                producerList.add(producerId);

                raf = new RandomAccessFile(seatFile, "r");
                String line = raf.readLine();
                parseSeatFile(line, producerId);
            } catch (IOException e) { e.printStackTrace();}
        }


    }

    private void parseSeatFile(String line, String producerId) {
        String[] pairs = line.split(",");
        for (String pair: pairs) {
            String[] kvs = pair.split(":");
            String[] vals = kvs[1].split("#");

            int offset = Integer.parseInt(vals[0]);
            int length = Integer.parseInt(vals[1]);
            if (!bucketStartInfo.containsKey(kvs[0]))
                bucketStartInfo.put(kvs[0], new ArrayList<>());

            List<Triple> triples = bucketStartInfo.get(kvs[0]);
            MetaInfo metaInfo = new MetaInfo(offset, length);
            triples.add(new Triple(producerId, metaInfo));
        }
    }

    public static MessageBroker getInstance(KeyValue properties) {
        if (INSTANCE == null) {
            synchronized (MessageBroker.class) {
                if (INSTANCE == null)
                    INSTANCE  = new MessageBroker(properties);
                    instanceCnt++;
            }
        }
        return INSTANCE;
    }




}
