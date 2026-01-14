package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService seckill_order_executor = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        seckill_order_executor.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    // 1. Retrieve order information from the message queue XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2. Check whether the message is successfully retrieved
                    if (list == null || list.isEmpty()) {
                        // If retrieval fails, it means there is no message, continue to the next loop
                        continue;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 3. If retrieval succeeds, create the order
                    handleVoucherOrder(voucherOrder);
                    // 4. Acknowledge (ACK) the message SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("Handle order exceptions", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1. Retrieve order information from the pending-list XREADGROUP GROUP g1 c1 COUNT 1  STREAMS streams.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2. Check whether the message is successfully retrieved
                    if (list == null || list.isEmpty()) {
                        // If retrieval fails, it means there is no message, finish the loop
                        break;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 3. If retrieval succeeds, create the order
                    handleVoucherOrder(voucherOrder);
                    // 4. Acknowledge (ACK) the message SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("Handle pending-list order exceptions", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    /*private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("Handle order exceptions", e);
                }
            }
        }
    }*/

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            Long userId = voucherOrder.getUserId();
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            // Acquire the lock
            boolean isLock = lock.tryLock();
            // Check if it is successful to acquire the lock
            if (!isLock) {
                // fail
                log.error("Each user can place only one order");
                return;
            }
            try {
                proxy.createVoucherOrder(voucherOrder);
            } finally {
                lock.unlock();
            }
        }

        private IVoucherOrderService proxy;

        @Override
        public Result seckillVoucher(Long voucherId) {
            Long userId = UserHolder.getUser().getId();
            long orderId = redisIdWorker.nextId("order");
            // 1. Execute the Lua script
            Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString(), String.valueOf(orderId));
            // 2. Check whether the result equals 0
            int r = result.intValue();
            if (r != 0) {
                // 2.1 Not 0: means the user is not eligible to purchase
                return Result.fail(r == 1 ? "Insufficient stock" : "Each user can place only one order");
            }
            proxy = (IVoucherOrderService) AopContext.currentProxy();
            // 3. Return order ID
            return Result.ok(orderId);
        }

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1. Execute the Lua script
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        // 2. Check whether the result equals 0
        int r = result.intValue();
        if (r != 0) {
            // 2.1 Not 0: means the user is not eligible to purchase
            return Result.fail(r == 1 ? "Insufficient stock" : "Each user can place only one order");
        }
        // 2.2 Equals 0: eligible to purchase, save order information to the message queue
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);

        orderTasks.add(voucherOrder);

        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 3. Return order ID
        return Result.ok(orderId);
    }*/

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 1. Query voucher
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2. Check whether the seckill has started
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("seckill has not yet started");
        }
        // 3. Check whether the seckill has ended
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("seckill has already finished");
        }
        // 4. Check whether the stock is sufficient
        if (voucher.getStock() < 1) {
            return Result.fail("Insufficient stock");
        }

        Long userId = UserHolder.getUser().getId();
        // Create lock object
        // SimpleRedisLock lock = new SimpleRedisLock("order" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // Acquire the lock
        boolean isLock = lock.tryLock();
        // Check if it is successful to acquire the lock
        if (!isLock) {
            // fail
            return Result.fail("Each person is allowed only one order.");
        }

        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
    }*/

        @Transactional
        public void createVoucherOrder(VoucherOrder voucherOrder) {
            // 5. One order per user
            Long userId = voucherOrder.getUserId();

            // 5.1 Query order
            int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
            // 5.2 Check whether it exists
            if (count > 0) {
                log.error("The user has already made a purchase.");
                return;
            }

            // 6. Deduct stock
            boolean success = seckillVoucherService.update().setSql("stock = stock - 1").eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
            if (!success) {
                log.error("Insufficient stock");
                return;
            }

            save(voucherOrder);
        }
    }
