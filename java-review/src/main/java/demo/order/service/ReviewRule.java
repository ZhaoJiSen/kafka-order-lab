package demo.order.service;

import org.springframework.stereotype.Component;

@Component
public class ReviewRule {
  public Decision decide(long totalCents) {
    if (totalCents <= 0) throw new IllegalArgumentException("金额必须大于 0");
    return totalCents <= 100000
        ? new Decision("CONFIRMED", "订单金额不超过 1000 元，演示审核通过")
        : new Decision("REJECTED", "订单金额超过 1000 元，演示审核拒绝");
  }

  public record Decision(String status, String reason) {}
}
