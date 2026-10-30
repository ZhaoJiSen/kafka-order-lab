package demo.order.messaging.event;

import demo.order.service.ReviewRule;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** 发回 PHP 的消息结构；与数据库实体分开。 */
public record OrderReviewedEvent(
    String eventId,
    String eventType,
    int version,
    String source,
    String occurredAt,
    String correlationId,
    String causationId,
    Data data) {

  public static OrderReviewedEvent create(
      OrderCreatedEvent event,
      ConsumerRecord<String, String> record,
      ReviewRule.Decision decision,
      Instant now) {
    return new OrderReviewedEvent(
        "evt-" + UUID.randomUUID(),
        "OrderReviewed",
        1,
        "java-review",
        now.toString(),
        event.data().orderId(),
        event.eventId(),
        new Data(
            event.data().orderId(),
            decision.status(),
            decision.reason(),
            new InputRecord(record.topic(), record.partition(), record.offset(), record.key())));
  }

  public record Data(String orderId, String status, String reason, InputRecord inputRecord) {}

  public record InputRecord(String topic, int partition, long offset, String key) {}
}
