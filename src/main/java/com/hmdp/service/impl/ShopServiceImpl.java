package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        // query pass through
        // Shop shop = queryWithPassThrough(id);

        // mutex lock solve Cache Breakdown
        // Shop shop = queryWithMutex(id);

        // logical expire solve Cache Breakdown
        Shop shop = queryWithLogicalExpire(id);
        if (shop == null) {
            return Result.fail("the shop does not exist");
        }
        // 7. Return the result
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 1. Query shop data from Redis cache
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2. Check whether the data exists
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        // 4. Cache hit, deserialize JSON to object
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5. Check whether the data is logically expired
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 Not expired, return shop data directly
            return shop;
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
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        // 6.4 Return expired shop data
        return shop;
    }

    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;

        // 1. Query cache
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2. Cache hit (normal data)
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 3. Cache hit (null placeholder)
        if (shopJson != null) {
            return null;
        }

        String lockKey = "lock:shop:" + id;
        Shop shop;

        boolean isLock = false;
        try {
            // 4. Acquire lock
            isLock = tryLock(lockKey);
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(id); // or use a loop
            }

            // 4.5 Double check cache after getting the lock
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            if (shopJson != null) {
                return null;
            }

            // 5. Query DB
            shop = getById(id);
            // simulate rebuild delay
            Thread.sleep(200);
            if (shop == null) {
                // cache null
                stringRedisTemplate.opsForValue()
                        .set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }

            // 6. Write cache
            stringRedisTemplate.opsForValue()
                    .set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

            return shop;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            // ✅ only unlock if YOU acquired the lock
            if (isLock) {
                unlock(lockKey);
            }
        }
    }

    public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 1. Query shop data from Redis cache
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2. Check whether the data exists
        if (StrUtil.isNotBlank(shopJson)) {
            // 3. If it exists, return it directly
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // check whether the target is null
        if (shopJson != null) {
            return null;
        }

        // 4. If it does not exist, query the database by id
        Shop shop = getById(id);
        // 5. If the data does not exist in the database, return an error
        if (shop == null) {
            // write null in redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        // 6. If it exists, write the data into Redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 7. Return the result
        return shop;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException{
        // 1. Query shop data from the database
        Shop shop = getById(id);
        Thread.sleep(200);
        // 2. Set logical expiration time
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3. Write data to Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("the shop id can't be empty!");
        }
        // 1. update database
        updateById(shop);

        // 2. delete cache
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }
}
