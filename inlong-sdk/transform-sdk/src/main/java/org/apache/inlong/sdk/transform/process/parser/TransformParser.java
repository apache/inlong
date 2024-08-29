package org.apache.inlong.sdk.transform.process.parser;

import net.sf.jsqlparser.expression.Expression;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(TYPE)
public @interface TransformParser {
    Class<? extends Expression>[] value();
}
