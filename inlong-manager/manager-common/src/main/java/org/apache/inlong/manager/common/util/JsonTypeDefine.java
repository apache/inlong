package org.apache.inlong.manager.common.util;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Define the Json type for JsonTypeInfo
 *
 * @see JsonTypeInfo
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface JsonTypeDefine {

    String value() default "";

    String desc() default "";

}