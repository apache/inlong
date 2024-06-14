## Overview
The Audit Sdk is used to count the receiving and sending volume of each module in real time according to the cycle, 
and the statistical results are sent to the audit access layer according to the cycle.

## Features
### Data uniqueness
The Audit Sdk will add a unique mark to each audit, which can be used to remove duplicates.

### Unified audit standard
The Audit Sdk uses log production time as the audit standard, 
which can ensure that each module is reconciled in accordance with the unified audit standard.

## Usage
### Configure Audit Proxy Addresses
- The Audit SDK will summarize the results according to the cycle  
and send them to the ip:port list set by the interface.
- There are two ways to set the address of the Audit Proxy, configure the address directly or get the address from the manager. Please choose one of the methods.
#### Configure Audit Proxy Addresses by fixed ip:port
If the ip:port of the AuditProxy is fixed, then this interface needs to be called once. 
If the AuditProxy changes in real time, then the business program needs to call this interface periodically to update.
```java
    HashSet<String> ipPortList = new HashSet<>();
    ipPortList.add("0.0.0.0:54041");
    AuditOperator.getInstance().setAuditProxy(ipPortList);
```
#### Configure Audit Proxy Addresses by InLong Manager
By configuring the InLong Manager's address, module information, and manager certification information, 
The Audit SDK will automatically fetch the Manager to obtain the address of the Audit Proxy.
```java
        String host = "127.0.0.1:8083"; // The manager address
        String secretId = "*****"; // Secret id
        String secretKey = "******";  // Secret key
        AuditOperator.getInstance().setAuditProxy(AuditComponent,host,secretId,secretKey); 
```
- Explain of AuditComponent 
```java
public enum AuditComponent {

    AGENT("Agent"), DATAPROXY("DataProxy"), SORT("Sort"), COMMON_AUDIT("Common");
    private final String component;

    /**
     * Constructor for the enum.
     *
     * @param component the name of the component
     */

    AuditComponent(String component) {
        this.component = component;
    }

    /**
     * Returns the name of the component.
     *
     * @return the name of the component
     */

    public String getComponent() {
        return component;
    }
}
```

### Add Audit Data
Call the add method for statistics, where the auditID parameter uniquely identifies an audit object,
inlongGroupID,inlongStreamID,logTime are audit dimensions, count is the number of items, size is the size, and logTime
is milliseconds.
#### Example for Agent to Add Audit Data
```java
    AuditOperator.getInstance().add(auditID, auditTag, inlongGroupID, inlongStreamID, logTime,
         count, size, auditVersion);
```
The scenario of supplementary recording of agent data, so the version number parameter needs to be passed in.
#### Example for DataProxy to Add Audit Data
```java
    AuditOperator.getInstance().add(auditID, auditTag, inlongGroupID, inlongStreamID, logTime,
        count, size, auditVersion);
```
The scenario of supplementary recording of DataProxy data, so the version number parameter needs to be passed in.
#### Example for Sort Flink to Add Audit Data
```java
    AuditReporterImpl auditReporter=new AuditReporterImpl();
        auditReporter.setAuditProxy(ipPortList);
        auditReporter.setAutoFlush(false);

        AuditDimensions dimensions;
        AuditValues values;
        auditReporter.add(dimensions, values);
```
```java
    AuditReporterImpl auditReporter=new AuditReporterImpl();
        auditReporter.setAuditProxy(ipPortList);
        auditReporter.setAutoFlush(false);
        auditReporter.add(isolateKey, auditID, auditTag, inlongGroupID, inlongStreamID,
         logTime, count, size, auditVersion)
```
In order to ensure the accuracy of auditing, each operator needs to create an auditAuditReporterImpl instance.
- Explain of AuditDimensions

| parameter      | description                                                                                          |
|----------------|--------------------------------------------------------------------------------------------------|
| auditID        | audit id,each module's reception and transmission will be assigned its own independent audit-id. |   
| logTime        | log time ,each module uses the log time of the data source uniformly                             |     
| auditVersion   | audit version                                                                                    |     
| isolateKey     | Flink Checkpoint id                                                                              |
| auditTag       | audit tag,Used to mark the same audit-id but different data sources and destinations             |     
| inlongGroupID  | inlongGroupID                                                                                    |
| inlongStreamID | inlongStreamID                                                                                   | 

- Explain of AuditValues

| parameter       | description       |
|----------|----------|
| count  | count  |   
| size | size   |     
| delayTime     | Data transmission delay,equal to current time minus log time |

#### Build Audit item ID
```java
        AuditManagerUtils.buildAuditId(AuditIdEnum baseAuditId,
        boolean success,
        boolean isRealtime,
        boolean discard,
        boolean retry);
```
| parameter       | description                                                                      |
|----------|------------------------------------------------------------------------------|
| baseAuditId  | Each module is assigned two baseAuditId. For details, please see AuditIdEnum |   
| success | Success and failure flags                                                    |     
| isRealtime     | Real-time and non-real-time flags                                            |
| discard     | Discard flag                                                                 |
| retry     | Retry flag                                                                   |