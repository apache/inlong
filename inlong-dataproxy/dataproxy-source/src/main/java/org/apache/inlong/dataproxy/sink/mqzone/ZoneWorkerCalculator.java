package org.apache.inlong.dataproxy.sink.mqzone;

public interface ZoneWorkerCalculator {
    AbstactZoneWorker calculator(String sinkName, int workerIndex, AbstractZoneSinkContext context);
}
