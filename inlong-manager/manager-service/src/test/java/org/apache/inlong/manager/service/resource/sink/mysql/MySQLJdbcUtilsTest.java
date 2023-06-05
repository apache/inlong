package org.apache.inlong.manager.service.resource.sink.mysql;

import org.apache.inlong.manager.service.ServiceBaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class MySQLJdbcUtilsTest extends ServiceBaseTest {

    @Test
    void testFilterSensitive() throws Exception {

        String url = "jdbc:mysql://127.0.0.1:3306?autoDeserialize=TRue&allowLoadLocalInfile=TRue&autoReconnect=true&allowUrlInLocalInfile=TRue&allowLoadLocalInfileInPath=/";
        url = MySQLJdbcUtils.filterSensitive(url);

        Assertions.assertEquals(url, "jdbc:mysql://127.0.0.1:3306?autoDeserialize=false&allowLoadLocalInfile=false&autoReconnect=true&allowUrlInLocalInfile=false");
    }
}