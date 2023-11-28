package org.apache.inlong.manager.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class JdbcSensitiveUrlUtils {


    /**
     * The sensitive param may lead the attack.
     */
    private static final Map<String, String> SENSITIVE_REPLACE_PARAM_MAP = new HashMap<String, String>() {

        {
            put("autoDeserialize", "false");
            put("allowLoadLocalInfile", "false");
            put("allowUrlInLocalInfile", "false");
        }
    };

    private static final Set<String> SENSITIVE_REMOVE_PARAM_MAP = new HashSet<String>() {

        {
            add("allowLoadLocalInfileInPath");
        }
    };

    /**
     * Filter the sensitive params for the given URL.
     *
     * @param url str may have some sensitive params
     * @return str without sensitive param
     */
    public static String filterSensitive(String url) {
        if (StringUtils.isBlank(url)) {
            return url;
        }

        try {
            String resultUrl = url;
            while (resultUrl.contains(InlongConstants.PERCENT)) {
                resultUrl = URLDecoder.decode(resultUrl, "UTF-8");
            }
            resultUrl = resultUrl.replaceAll(InlongConstants.REGEX_WHITESPACE, InlongConstants.EMPTY);

            if (resultUrl.contains(InlongConstants.QUESTION_MARK)) {
                StringBuilder builder = new StringBuilder();
                builder.append(StringUtils.substringBefore(resultUrl, InlongConstants.QUESTION_MARK));
                builder.append(InlongConstants.QUESTION_MARK);

                List<String> paramList = new ArrayList<>();
                String queryString = StringUtils.substringAfter(resultUrl, InlongConstants.QUESTION_MARK);
                for (String param : queryString.split(InlongConstants.AMPERSAND)) {
                    String key = StringUtils.substringBefore(param, InlongConstants.EQUAL);
                    String value = StringUtils.substringAfter(param, InlongConstants.EQUAL);

                    if (SENSITIVE_REMOVE_PARAM_MAP.contains(key) || SENSITIVE_REPLACE_PARAM_MAP.containsKey(key)) {
                        continue;
                    }

                    paramList.add(key + InlongConstants.EQUAL + value);
                }
                SENSITIVE_REPLACE_PARAM_MAP.forEach((key, value) -> paramList.add(key + InlongConstants.EQUAL + value));

                String params = StringUtils.join(paramList, InlongConstants.AMPERSAND);
                builder.append(params);
                resultUrl = builder.toString();
            }

            log.info("the origin url [{}] was replaced to: [{}]", url, resultUrl);
            return resultUrl;
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                    ErrorCodeEnum.SINK_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }
}
