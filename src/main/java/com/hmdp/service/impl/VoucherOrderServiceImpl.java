package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Override
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
        synchronized (userId.toString().intern()) {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5. One order per user
        Long userId = UserHolder.getUser().getId();

        // 5.1 Query order
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        // 5.2 Check whether it exists
        if (count > 0) {
            return Result.fail("The user has already made a purchase.");
        }

        // 6. Deduct stock
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!success) {
            return Result.fail("Insufficient stock");
        }

        // 7. Create order
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        // 7. Return order ID
        return Result.ok(orderId);
    }
}
