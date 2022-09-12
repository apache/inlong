package org.apache.inlong.manager.service.source;

import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.source.redis.RedisSource;
import org.apache.inlong.manager.pojo.source.redis.RedisSourceRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Redis source service test
 */
public class RedisSourceServiceTest extends ServiceBaseTest {

    private static final String hostname = "127.0.0.1";
    private static final Integer port = 6379;
    private static final String redisMode = "standalone";
    private static final String redisCommand = "get";
    private final String sourceName = "stream_source_service_test";

    @Autowired
    private StreamSourceService sourceService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    /**
     * Save source info
     */
    public Integer saveSource() {
        streamServiceTest.saveInlongStream(GLOBAL_GROUP_ID, GLOBAL_STREAM_ID, GLOBAL_OPERATOR);

        RedisSourceRequest sourceInfo = new RedisSourceRequest();
        sourceInfo.setInlongGroupId(GLOBAL_GROUP_ID);
        sourceInfo.setInlongStreamId(GLOBAL_STREAM_ID);
        sourceInfo.setSourceName(sourceName);
        sourceInfo.setSourceType(SourceType.REDIS);
        sourceInfo.setHostname(hostname);
        sourceInfo.setPort(port);
        sourceInfo.setRedisCommand(redisCommand);
        sourceInfo.setRedisMode(redisMode);
        return sourceService.save(sourceInfo, GLOBAL_OPERATOR);
    }

    @Test
    public void testSaveAndDelete() {
        Integer id = this.saveSource();
        Assertions.assertNotNull(id);

        boolean result = sourceService.delete(id, GLOBAL_OPERATOR);
        Assertions.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        Integer id = this.saveSource();
        StreamSource source = sourceService.get(id);
        Assertions.assertEquals(GLOBAL_GROUP_ID, source.getInlongGroupId());

        sourceService.delete(id, GLOBAL_OPERATOR);
    }

    @Test
    public void testGetAndUpdate() {
        Integer id = this.saveSource();
        StreamSource response = sourceService.get(id);
        Assertions.assertEquals(GLOBAL_GROUP_ID, response.getInlongGroupId());

        RedisSource redisSource = (RedisSource) response;
        RedisSourceRequest request = CommonBeanUtils.copyProperties(redisSource, RedisSourceRequest::new);
        System.out.println(request);
        boolean result = sourceService.update(request, GLOBAL_OPERATOR);
        Assertions.assertTrue(result);

        sourceService.delete(id, GLOBAL_OPERATOR);
    }

}
