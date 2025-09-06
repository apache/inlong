package org.apache.inlong.audit.tool.basemetric.util;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.datasource.pooled.PooledDataSource;

import javax.sql.DataSource;
import java.util.Properties;

public class AuditSQLUtil {
    private static SqlSessionFactory sqlSessionFactory;
    private static Properties appProperties;

    public static void initialize(Properties properties) {
        appProperties = properties;
        try {
            // Create a data source
            DataSource dataSource = createDataSourceFromProperties();

            // Create a SqlSessionFactory
            org.apache.ibatis.session.Configuration configuration = new org.apache.ibatis.session.Configuration();
            configuration.setEnvironment(new org.apache.ibatis.mapping.Environment(
                    "development",
                    new org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory(),
                    dataSource
            ));

            configuration.addMapper(org.apache.inlong.audit.tool.basemetric.mapper.AuditMapper.class);

            sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);

        } catch (Exception e) {
            throw new RuntimeException("Error initializing MyBatis with application properties", e);
        }
    }

    private static DataSource createDataSourceFromProperties() {
        String url = appProperties.getProperty("audit.data.source.url");
        String username = appProperties.getProperty("audit.data.source.username");
        String password = appProperties.getProperty("audit.data.source.password");

        PooledDataSource dataSource = new PooledDataSource();
        dataSource.setDriver("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        dataSource.setPoolMaximumActiveConnections(10);
        dataSource.setPoolMaximumIdleConnections(5);

        return dataSource;
    }

    public static SqlSession getSqlSession() {
        if (sqlSessionFactory == null) {
            throw new IllegalStateException("MyBatisUtil not initialized. Call initialize() first.");
        }
        return sqlSessionFactory.openSession();
    }
}