package demo.order.entity;

import demo.order.messaging.event.OrderCreatedEvent;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** 已处理事件及其 Kafka 位置，用 eventId 识别重复消息。 */
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {
  @Id
  @Column(name = "event_id", length = 100)
  private String eventId;

  @Column(name = "order_id", nullable = false, length = 100)
  private String orderId;

  @Column(nullable = false, length = 200)
  private String topic;

  @Column(name = "partition_id", nullable = false)
  private int partition;

  @Column(name = "offset_id", nullable = false)
  private long offset;

  @Lob
  @Column(nullable = false)
  private String payload;

  @JdbcTypeCode(SqlTypes.TIMESTAMP)
  @Column(name = "processed_at", nullable = false)
  private Instant processedAt;

  protected ProcessedEvent() {}

  public ProcessedEvent(
      OrderCreatedEvent event, ConsumerRecord<String, String> record, Instant processedAt) {
    this.eventId = event.eventId();
    this.orderId = event.data().orderId();
    this.topic = record.topic();
    this.partition = record.partition();
    this.offset = record.offset();
    this.payload = record.value();
    this.processedAt = processedAt;
  }

  public String getEventId() { return eventId; }
  public String getOrderId() { return orderId; }
  public String getTopic() { return topic; }
  public int getPartition() { return partition; }
  public long getOffset() { return offset; }
  public String getPayload() { return payload; }
  public Instant getProcessedAt() { return processedAt; }
}
