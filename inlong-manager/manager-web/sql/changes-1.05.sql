USE `apache_inlong_manager`;


ALTER TABLE `inlong_group`
    ADD COLUMN `data_report_type` int(4) DEFAULT '0' COMMENT 'Data report type. 0: report to DataProxy and respond when the DataProxy received data. 1: report to DataProxy and respond after DataProxy sends data. 2: report to MQ and respond when the MQ received data';


ALTER TABLE `inlong_cluster_node`
    ADD COLUMN `node_load` int(11) DEFAULT '-1' COMMENT 'Current load value of the node';


ALTER TABLE `inlong_cluster_node`
    ADD COLUMN `node_tags` varchar(512) DEFAULT NULL COMMENT 'Cluster node tag, separated by commas, only uniquely identified by parent_id and ip';


ALTER TABLE `stream_source`
    ADD COLUMN `inlong_cluster_node_tag` varchar(512) DEFAULT NULL COMMENT 'Cluster node tag';