package org.apache.inlong.audit;

//import org.apache.inlong.audit.protocol.AuditApi;

import org.apache.inlong.audit.protocol.AuditApi;
import org.apache.inlong.audit.send.SenderManager;
import org.apache.inlong.audit.util.Config;
import org.apache.inlong.audit.util.StatInfo;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class AuditImp {
    private static AuditImp auditImp = new AuditImp();
    private static final String FIELD_SEPARATORS = ":";
    private ConcurrentHashMap<String, StatInfo> countMap = new ConcurrentHashMap<String, StatInfo>();
    private HashMap<String, StatInfo> threadSumMap = new HashMap<String, StatInfo>();
    private ConcurrentHashMap<String, StatInfo> deleteCountMap = new ConcurrentHashMap<String, StatInfo>();
    private List<String> deleteKeyList = new ArrayList<String>();
    private Config config = new Config();
    private Long sdkTime;
    private int packageId = 1;
    private int dataId = 0;
    private static final int BATCH_NUM = 100;
    private SenderManager manager;
    private String configFile;
    private Timer timer = new Timer();
    private TimerTask timerTask = new TimerTask() {
        @Override
        public void run() {
            try {
                sendReport();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    public static AuditImp getInstance() {
        return auditImp;
    }

    /**
     * init
     */
    public void init(String configFile) {
        this.configFile = configFile;
        config.init();
        timer.schedule(timerTask, 1000 * 60, 1000 * 60);
        this.manager = new SenderManager(config);
        this.manager.reload(this.configFile);
    }

    /**
     * api
     *
     * @param auditID
     * @param inlongGroupID
     * @param inlongStreamID
     * @param logTime
     * @param count
     * @param size
     */
    public void add(int auditID, String inlongGroupID, String inlongStreamID, Long logTime, long count, long size) {
        long delayTime = System.currentTimeMillis() - logTime;
        String key = (logTime / (60 * 1000)) + FIELD_SEPARATORS + inlongGroupID + FIELD_SEPARATORS
                + inlongStreamID + FIELD_SEPARATORS + auditID;
        addByKey(key, count, size, delayTime);
    }

    /**
     * add by key
     *
     * @param key
     * @param count
     * @param size
     * @param delayTime
     */
    private void addByKey(String key, long count, long size, long delayTime) {
        try {
            if (countMap.get(key) == null) {
                countMap.put(key, new StatInfo(0L, 0L, 0L));
            }
            countMap.get(key).count.addAndGet(count);
            countMap.get(key).size.addAndGet(size);
            countMap.get(key).delay.addAndGet(delayTime * count);
        } catch (Exception e) {
            return;
        }
    }

    /**
     * Report audit data
     */
    private synchronized void sendReport() {
        manager.clearBuffer();
        this.manager.reload(this.configFile);
        resetStat();
        // Retrieve statistics from the list of objects without statistics to be eliminated
        for (Map.Entry<String, StatInfo> entry : this.deleteCountMap.entrySet()) {
            this.sumThreadGroup(entry.getKey(), entry.getValue());
        }
        this.deleteCountMap.clear();
        this.deleteKeyList.clear();
        for (Map.Entry<String, StatInfo> entry : countMap.entrySet()) {
            String key = entry.getKey();
            StatInfo value = entry.getValue();
            // If there is no data, enter the list to be eliminated
            if (value.count.get() == 0) {
                this.deleteKeyList.add(key);
                continue;
            }
            this.sumThreadGroup(key, value);
        }

        // Clean up obsolete statistical data objects
        for (String key : this.deleteKeyList) {
            StatInfo value = this.countMap.remove(key);
            this.deleteCountMap.put(key, value);
        }
        this.deleteKeyList.clear();
        sdkTime = Calendar.getInstance().getTimeInMillis();
        AuditApi.AuditMessageHeader mssageHeader = AuditApi.AuditMessageHeader.newBuilder()
                .setIp(config.getLocalIP()).setDockerId(config.getDockerId())
                .setThreadId(String.valueOf(Thread.currentThread().getId()))
                .setSdkTs(sdkTime).setPacketId(packageId)
                .build();
        AuditApi.AuditRequest.Builder requestBulid = AuditApi.AuditRequest.newBuilder();
        requestBulid.setMsgHeader(mssageHeader);
        for (Map.Entry<String, StatInfo> entry : threadSumMap.entrySet()) {
            String[] keyArray = entry.getKey().split(FIELD_SEPARATORS);
            long logTime = Long.parseLong(keyArray[0]) * 60;
            String inlongGroupID = keyArray[1];
            String inlongStreamID = keyArray[2];
            String auditID = keyArray[3];
            StatInfo value = entry.getValue();
            AuditApi.AuditMessageBody mssageBody = AuditApi.AuditMessageBody.newBuilder()
                    .setLogTs(logTime).setInlongGroupId(inlongGroupID)
                    .setInlongStreamId(inlongStreamID).setAuditId(auditID)
                    .setCount(value.count.get()).setSize(value.size.get())
                    .setDelay(value.delay.get())
                    .build();
            requestBulid.addMsgBody(mssageBody);
            if (dataId++ >= BATCH_NUM) {
                dataId = 0;
                packageId++;
                manager.send(sdkTime, requestBulid.build());
                requestBulid.clearMsgBody();
            }
        }
        if (requestBulid.getMsgBodyCount() > 0) {
            manager.send(sdkTime, requestBulid.build());
        }
    }

    /**
     * Summary
     *
     * @param key
     * @param statInfo
     */
    private void sumThreadGroup(String key, StatInfo statInfo) {
        if (threadSumMap.get(key) == null) {
            threadSumMap.put(key, new StatInfo(0, 0, 0));
        }
        long count = statInfo.count.getAndSet(0);
        long size = statInfo.size.getAndSet(0);
        long delay = statInfo.delay.getAndSet(0);

        threadSumMap.get(key).count.addAndGet(count);
        threadSumMap.get(key).size.addAndGet(size);
        threadSumMap.get(key).delay.addAndGet(delay);
    }

    /**
     * Reset statistics
     */
    private void resetStat() {
        dataId = 0;
        packageId = 1;
    }
}
