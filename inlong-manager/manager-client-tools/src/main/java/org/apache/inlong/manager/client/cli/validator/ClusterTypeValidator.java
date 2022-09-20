package org.apache.inlong.manager.client.cli.validator;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import org.apache.inlong.manager.common.enums.ClusterType;

/**
 * Class for cluster type verification.
 */
public class ClusterTypeValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        switch (value) {
            case ClusterType.PULSAR:
            case ClusterType.DATAPROXY:
            case ClusterType.TUBEMQ:
            case ClusterType.AGENT:
            case ClusterType.KAFKA:
                return;
            default:
                String msg = "should be one of the following values:\n"
                        + "\tPULSAR, DATAPROXY, TUBEMQ, AGENT, KAFKA";
                throw new ParameterException("Parameter " + name + msg);
        }
    }
}
