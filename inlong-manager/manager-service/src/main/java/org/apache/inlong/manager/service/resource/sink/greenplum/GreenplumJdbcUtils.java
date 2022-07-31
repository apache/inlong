package org.apache.inlong.manager.service.resource.sink.greenplum;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Utils for Greenplum JDBC.
 */
public class GreenplumJdbcUtils {

    private static final String GREENPLUM_JDBC_PREFIX = "jdbc:postgresql";

    private static final String GREENPLUM_DRIVER_CLASS = "org.postgresql.Driver";

    private static final Logger LOG = LoggerFactory.getLogger(GreenplumJdbcUtils.class);

    /**
     * Get Greenplum connection from the url and user
     */
    public static Connection getConnection(String url, String user, String password)
            throws Exception {
        if (StringUtils.isBlank(url) || !url.startsWith(GREENPLUM_JDBC_PREFIX)) {
            throw new Exception("Greenplum server URL was invalid, it should start with jdbc:postgresql");
        }
        Connection conn;
        try {
            Class.forName(GREENPLUM_DRIVER_CLASS);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            String errorMsg = "get Greenplum connection error, please check postgres jdbc url, username or password!";
            LOG.error(errorMsg, e);
            throw new Exception(errorMsg + " other error msg: " + e.getMessage());
        }
        if (conn == null) {
            throw new Exception("get Greenplum connection failed, please contact administrator");
        }
        LOG.info("get Greenplum connection success, url={}", url);
        return conn;
    }

    /**
     * Execute SQL command on Greenplum.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param sql SQL string to be executed
     * @throws Exception on execute SQL error
     */
    public static void executeSql(Connection conn, String sql) throws Exception {
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        LOG.info("execute sql [{}] success", sql);
        stmt.close();
    }

    /**
     * Execute query SQL on Greenplum.
     *
     * @param conn JDBC Connection  {@link Connection}
     * @param sql SQL string to be executed
     * @return {@link ResultSet}
     * @throws Exception on execute query SQL error
     */
    public static ResultSet executeQuerySql(Connection conn, String sql)
            throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery(sql);
        LOG.info("execute sql [{}] success", sql);
        return resultSet;
    }




    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://192.168.162.136:5432/testdb";
        String user = "jie_li";
        String passwd = "123456";
        Connection conn = GreenplumJdbcUtils.getConnection(url, user, passwd);
        String sql = "select * from public.test1";
        GreenplumJdbcUtils.executeSql(conn,sql);
    }

}
