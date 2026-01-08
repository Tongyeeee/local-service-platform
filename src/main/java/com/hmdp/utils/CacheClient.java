package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * ClassName:CacheClient
 * Package: com.hmdp.utils
 * Description:
 *
 * @Autor: Tong
 * @Create: 07.01.26 - 14:13
 * @Version: v1.0
 *
 */
@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // Set logical expiration time
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        // Write to Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1. Query shop data from Redis cache
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2. Check whether the data exists
        if (StrUtil.isNotBlank(json)) {
            // 3. If it exists, return it directly
            return JSONUtil.toBean(json, type);
        }
        // check whether the target is null
        if (json != null) {
            return null;
        }

        // 4. If it does not exist, query the database by id
        R r = dbFallback.apply(id);
        // 5. If the data does not exist in the database, return an error
        if (r == null) {
            // write null in redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        // 6. If it exists, write the data into Redis
        this.set(key, r, time, unit);
        // 7. Return the result
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1. Query shop data from Redis cache
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2. Check whether the data exists
        if (StrUtil.isBlank(json)) {
            return null;
        }
        // 4. Cache hit, deserialize JSON to object
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5. Check whether the data is logically expired
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 Not expired, return shop data directly
            return r;
        }
        // 5.2 Expired, trigger cache rebuild
        // 6. Cache rebuild
        // 6.1 Acquire mutex lock
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2 Check whether the lock is acquired successfully
        if (isLock) {
            // 6.3 If successful, start a separate thread to rebuild the cache
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // Query db
                    R r1 = dbFallback.apply(id);
                    // Write to Redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        // 6.4 Return expired shop data
        return r;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
