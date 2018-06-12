package com.ipet.queue.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ipet.queue.IQueueConsumer;
import com.ipet.queue.redis.annotation.RedisQueueConsumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * @author 杨斌冰-工具组-技术中心
 * <p>
 * 2018/3/1 15:31
 */
public abstract class AbstractRedisQueueConsumer implements IQueueConsumer {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    private String queueName;

    public abstract void doConsume(String message);

    @Override
    public void consume() throws Exception {
        if (this.getClass().isAnnotationPresent(RedisQueueConsumer.class)) {
            RedisQueueConsumer redisQueueConsumer = this.getClass().getAnnotation(RedisQueueConsumer.class);
            queueName = StringUtils.isNotBlank(redisQueueConsumer.value()) ? redisQueueConsumer.value() : redisQueueConsumer.queueName();
            if (StringUtils.isBlank(queueName)) {
                throw new Exception("Redis Queue Consumer Java Config Error.");
            }
            RedisConnection connection = redisConnectionFactory.getConnection();
            while (true) {
                List<byte[]> message = connection.bLPop(0, queueName.getBytes("UTF-8"));
                if (message != null && message.size() > 1) {
                    String item = new String(message.get(1));
                    this.doConsume(item);
                }
            }
        } else {
            throw new Exception("Redis Queue Consumer Java Config Error.");
        }
    }

    @PostConstruct
    public void init() {
        logger.info("The Consumer in Queue [{}] Started.", this.getClass().getName());
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                this.consume();
            } catch (Exception e) {
                logger.error("The Consumer in Queue [" + this.queueName + "] Error.", e);
            }
        });
    }
}
