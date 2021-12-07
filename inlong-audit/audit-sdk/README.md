# Description
## overview
The audit sdk is used to count the receiving and sending volume of each module in real time according to the cycle, 
and the statistical results are sent to the audit access layer according to the cycle.

##features
### data uniqueness
The audit sdk will add a unique mark to each audit audit, which can be used to remove duplicates.

### unified audit standard
The audit sdk uses log production time as the audit standard, 
which can ensure that each module is reconciled in accordance with the unified audit standard.

## usage
### init
Call the init method to initialize. Among them, the parameter configFile is to audit the ip port configuration file of the access layer, 
and each ip port is separated by a new line.

### add
Call the add method for statistics, where the auditID parameter uniquely identifies an audit object,
inlongGroupID,inlongStreamID,logTime are audit dimensions, count is the number of items, size is the size, and logTime is milliseconds.