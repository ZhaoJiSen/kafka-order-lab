package demo.order.messaging;

import demo.order.repository.OutboxEventRepository;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxPublisher {
  private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
  private final OutboxEventRepository outbox;
  private final KafkaTemplate<String, String> kafka;

  public OutboxPublisher(OutboxEventRepository outbox, KafkaTemplate<String, String> kafka) {
    this.outbox = outbox;
    this.kafka = kafka;
  }

  @Scheduled(fixedDelay = 1000)
  public void publishPending() {
    var events = outbox.findTop20BySentAtIsNullOrderByCreatedAtAscEventIdAsc();
    for (var event : events) {
      try {
        // send 返回一个 future 类型，.get 等待发送结果 最多 12s
        var sent = kafka.send(event.getTopic(), event.getOrderId(), event.getPayload())
            .get(12, TimeUnit.SECONDS)
            .getRecordMetadata();

        // Kafka 确认后，再把实际分区、Offset 和发送时间存回数据库。
        event.markSent(sent.partition(), sent.offset(), Instant.now());
        outbox.saveAndFlush(event);
        log.info(
            "Java → Kafka：OrderReviewed 已确认写入 eventId={} topic={} partition={} offset={} key={}",
            event.getEventId(), event.getTopic(), sent.partition(), sent.offset(), event.getOrderId());
      } catch (Exception error) {
        if (error instanceof InterruptedException) Thread.currentThread().interrupt();
        log.warn("Java 发送结果失败，保留 Outbox 事件等待重试 eventId={}", event.getEventId(), error);
        break;
      }
    }
  }
}
