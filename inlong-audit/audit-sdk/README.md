 # Description
## overview
The audit sdk is used to count the receiving and sending volume of each module in real time according to the cycle, 
and the statistical results are sent to the audit access layer according to the cycle.

## features
### data uniqueness
The audit sdk will add a unique mark to each audit audit, which can be used to remove duplicates.

### unified audit standard
The audit sdk uses log production time as the audit standard, 
which can ensure that each module is reconciled in accordance with the unified audit standard.

## usage
### setAuditProxy
Set the audit access layer ip:port list. The audit sdk will summarize the results according to the cycle 
and send them to the ip:port list set by the interface.
If the ip:port of the audit access layer is fixed, then this interface needs to be called once. 
If the audit access changes in real time, then the business program needs to call this interface periodically to update
```java
    HashSet<String> ipPortList=new HashSet<>();
    ipPortList.add("0.0.0.0:54041");
    AuditOperator.getInstance().setAuditProxy(ipPortList);
```

### add api
Call the add method for statistics, where the auditID parameter uniquely identifies an audit object,
inlongGroupID,inlongStreamID,logTime are audit dimensions, count is the number of items, size is the size, and logTime
is milliseconds.

#### Example of add api for agent
```java
    AuditOperator.getInstance().add(auditID,auditTag,inlongGroupID,inlongStreamID,logTime,
        count,size,auditVersion);
```
The scenario of supplementary recording of agent data, so the version number parameter needs to be passed in.
#### Example of add api for dataproxy
```java
    AuditOperator.getInstance().add(auditID,auditTag,inlongGroupID,inlongStreamID,logTime,
        count,size,auditVersion);
```
The scenario of supplementary recording of dataproxy data, so the version number parameter needs to be passed in.

#### Example of add api for sort
```java
    AuditReporterImpl auditReporter=new AuditReporterImpl();
        auditReporter.setAuditProxy(ipPortList);

        AuditDimensions dimensions;
        AuditValues values;
        auditReporter.add(dimensions,values);
```

##### AuditReporterImpl
In order to ensure the accuracy of auditing, each operator needs to create an auditAuditReporterImpl instance.
##### Explain of AuditDimensions
| parameter      | meaning                                                                                          |
|----------------|--------------------------------------------------------------------------------------------------|
| auditID        | audit id,each module's reception and transmission will be assigned its own independent audit-id. |   
| logTime        | log time ,each module uses the log time of the data source uniformly                             |     
| auditVersion   | audit version                                                                                    |     
| isolateKey     | Flink checkpoint id                                                                              |
| auditTag       | audit tag,Used to mark the same audit-id but different data sources and destinations             |     
| inlongGroupID  | inlongGroupID                                                                                    |
| inlongStreamID | inlongStreamID                                                                                   | 

##### Explain of AuditValues
| parameter       | meaning       |
|----------|----------|
| count  | count  |   
| size | size   |     
| delayTime     | Data transmission delay,equal to current time minus log time |