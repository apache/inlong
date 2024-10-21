## Overview

This SDK is used to collect dirty data and store it in a designated storage location.

## Features

### Independent SDK

Independent SDK, not dependent on platform specific libraries (such as Flink), can be used by Agent,
DataProxy, Sort modules.

### Scalable multiple data storage options

Dirty data can be stored in various different storage locations (currently only supports sending to
DataProxy).

## Usage

### Create DirtyDataCollector object

```java
        Map<String, String> configMap = new ConcurrentHashMap<>();
        // Enable dirty data collection
        configMap.put(DIRTY_COLLECT_ENABLE, "true");
        // If ignore error messages during dirty data collection
        configMap.put(DIRTY_SIDE_OUTPUT_IGNORE_ERRORS, "true");
        // The storage where dirty data will be stored currently only supports 'inlong', 
        // which means sending the data to DataSroxy
        configMap.put(DIRTY_SIDE_OUTPUT_CONNECTOR, "inlong");
        // The labels of dirty side-output, format is 'key1=value1&key2=value2'
        configMap.put(DIRTY_SIDE_OUTPUT_LABELS, "key1=value1&key2=value2");
        // The log tag of dirty side-output, it supports variable replace like '${variable}'.
        configMap.put(DIRTY_SIDE_OUTPUT_LOG_TAG, "DirtyData");
        Configure config = new Configure(configMap);

        DirtyDataCollector collecter = new DirtyDataCollector();
        collector.open(config);
```

### Collect dirty data

```java
    // In fact, the dirty data we encounter is often parsed incorrectly, 
    // so we use byte [] as the format for dirty data.
    byte[] dirtyData = "xxxxxxxxxyyyyyyyyyyyyyy".getBytes(StandardCharsets.UTF_8);
    // Here, incorrect types can be marked, such as missing fields, type errors, or unknown errors, etc.
    String dirtyType = "Undefined";
    // Details of errors can be passed here.
    Throwable error = new Throwable();
    collector.invoke(dirtyData, dirtyType, error);
```

                                           |