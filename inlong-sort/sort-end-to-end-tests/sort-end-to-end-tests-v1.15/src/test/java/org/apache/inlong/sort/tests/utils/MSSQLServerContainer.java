package org.apache.inlong.sort.tests.utils;

import com.google.common.collect.Sets;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.LicenseAcceptance;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class MSSQLServerContainer extends JdbcDatabaseContainer {
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("mcr.microsoft.com/mssql/server");
    /** @deprecated */
    @Deprecated
    public static final String DEFAULT_TAG = "2017-CU12";
    public static final String NAME = "sqlserver";
    public static final String IMAGE;
    public static final Integer MS_SQL_SERVER_PORT;
    static final String DEFAULT_USER = "SA";
    static final String DEFAULT_PASSWORD = "A_Str0ng_Required_Password";
    private String password;
    private static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 240;
    private static final Pattern[] PASSWORD_CATEGORY_VALIDATION_PATTERNS;

    /** @deprecated */
    @Deprecated
    public MSSQLServerContainer() {
        this(DEFAULT_IMAGE_NAME.withTag("2017-CU12"));
    }

    public MSSQLServerContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public MSSQLServerContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        this.password = "A_Str0ng_Required_Password";
        dockerImageName.assertCompatibleWith(new DockerImageName[]{DEFAULT_IMAGE_NAME});
        this.withStartupTimeoutSeconds(240);
        this.withConnectTimeoutSeconds(240);
        this.addExposedPort(MS_SQL_SERVER_PORT);
    }

    public Set<Integer> getLivenessCheckPortNumbers() {
        return Sets.newHashSet(new Integer[]{MS_SQL_SERVER_PORT});
    }

    protected void configure() {
        if (!this.getEnvMap().containsKey("ACCEPT_EULA")) {
            LicenseAcceptance.assertLicenseAccepted(this.getDockerImageName());
            this.acceptLicense();
        }

        this.addEnv("SA_PASSWORD", this.password);
        this.addEnv("MSSQL_AGENT_ENABLED", "true");
        this.addFixedExposedPort(14433, MS_SQL_SERVER_PORT);
    }

    public MSSQLServerContainer acceptLicense() {
        this.addEnv("ACCEPT_EULA", "Y");
        return this;
    }

    public String getDriverClassName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    protected String constructUrlForConnection(String queryString) {

        if (urlParameters.keySet().stream().map(sp -> ((String)sp).toLowerCase()).noneMatch("encrypt"::equals)) {
            urlParameters.put("encrypt", "false");
        }
        return super.constructUrlForConnection(queryString);
    }

    public String getJdbcUrl() {
        String additionalUrlParams = this.constructUrlParameters(";", ";");
        return "jdbc:sqlserver://" + this.getHost() + ":" + this.getMappedPort(MS_SQL_SERVER_PORT) + additionalUrlParams;
    }

    public String getUsername() {
        return "SA";
    }

    public String getPassword() {
        return this.password;
    }

    public String getTestQueryString() {
        return "SELECT 1";
    }

    public MSSQLServerContainer withPassword(String password) {
        this.checkPasswordStrength(password);
        this.password = password;
        return this;
    }

    private void checkPasswordStrength(String password) {
        if (password == null) {
            throw new IllegalArgumentException("Null password is not allowed");
        } else if (password.length() < 8) {
            throw new IllegalArgumentException("Password should be at least 8 characters long");
        } else if (password.length() > 128) {
            throw new IllegalArgumentException("Password can be up to 128 characters long");
        } else {
            long satisfiedCategories = Stream.of(PASSWORD_CATEGORY_VALIDATION_PATTERNS).filter((p) -> {
                return p.matcher(password).find();
            }).count();
            if (satisfiedCategories < 3L) {
                throw new IllegalArgumentException("Password must contain characters from three of the following four categories:\n - Latin uppercase letters (A through Z)\n - Latin lowercase letters (a through z)\n - Base 10 digits (0 through 9)\n - Non-alphanumeric characters such as: exclamation point (!), dollar sign ($), number sign (#), or percent (%).");
            }
        }
    }

    static {
        IMAGE = DEFAULT_IMAGE_NAME.getUnversionedPart();
        MS_SQL_SERVER_PORT = 1433;
        PASSWORD_CATEGORY_VALIDATION_PATTERNS = new Pattern[]{Pattern.compile("[A-Z]+"), Pattern.compile("[a-z]+"), Pattern.compile("[0-9]+"), Pattern.compile("[^a-zA-Z0-9]+", 2)};
    }
}

