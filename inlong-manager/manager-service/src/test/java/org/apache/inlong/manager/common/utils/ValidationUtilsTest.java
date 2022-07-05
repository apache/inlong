package org.apache.inlong.manager.common.utils;

import org.apache.inlong.manager.common.pojo.cluster.ClusterTagRequest;
import org.apache.inlong.manager.common.pojo.common.UpdateValidation;
import org.apache.inlong.manager.common.util.ValidationUtils;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Resource;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;

/**
 * Test {@link ValidationUtils}
 */
class ValidationUtilsTest extends ServiceBaseTest {

    @Resource
    Validator validator;

    @Test
    void testValidate4Group() {
        ConstraintViolationException violationException = Assertions.assertThrows(
                ConstraintViolationException.class,
                () -> ValidationUtils.validate(validator, new ClusterTagRequest(), UpdateValidation.class));

        Assertions.assertTrue(violationException.getMessage().contains("id: id must not be null"));
    }

    @Test
    void testValidate() {
        ConstraintViolationException violationException = Assertions.assertThrows(
                ConstraintViolationException.class,
                () -> ValidationUtils.validate(validator, new ClusterTagRequest()));

        Assertions.assertEquals("clusterTag: clusterTag cannot be blank", violationException.getMessage());
    }

}
