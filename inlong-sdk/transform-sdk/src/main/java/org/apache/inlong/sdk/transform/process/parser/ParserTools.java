package org.apache.inlong.sdk.transform.process.parser;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Set;

@Slf4j
public class ParserTools {
    private static final String PARSER_PATH = "org.apache.inlong.sdk.transform.process.parser";
    private final static Map<Class<? extends Expression>, Class<?>> parserMap = Maps.newConcurrentMap();

    static{
        init();
    }
    private static void init() {
        Reflections reflections = new Reflections(PARSER_PATH, Scanners.TypesAnnotated);
        Set<Class<?>> clazzSet = reflections.getTypesAnnotatedWith(TransformParser.class);
        for (Class<?> clazz : clazzSet) {
            if (ValueParser.class.isAssignableFrom(clazz)) {
                TransformParser annotation = clazz.getAnnotation(TransformParser.class);
                if (annotation == null) {
                    continue;
                }
                Class<? extends Expression>[] values = annotation.value();
                for (Class<? extends Expression> value : values) {
                    if (value==null) {
                        continue;
                    }
                    parserMap.compute(value, (name, former) -> {
                        if (former != null) {
                            log.warn("find a conflict parser named [{}], the former one is [{}], new one is [{}]",
                                    name, former.getName(), clazz.getName());
                        }
                        return clazz;
                    });
                }
            }
        }
    }

    public static ValueParser getTransformParser(Expression expr) {
        Class<?> clazz = parserMap.get(expr.getClass());
        if (clazz == null) {
            return new ColumnParser((Column) expr);
        }
        try {
            Constructor<?> constructor = clazz.getDeclaredConstructor(expr.getClass());
            return (ValueParser) constructor.newInstance(expr);
        } catch (NoSuchMethodException e) {
            log.error("transform parser {} needs one constructor that accept one params whose type is {}",
                    clazz.getName(), expr.getClass().getName(), e);
            throw new RuntimeException(e);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
