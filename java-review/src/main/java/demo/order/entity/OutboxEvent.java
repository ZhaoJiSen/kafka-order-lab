package demo.order.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** 已写入数据库、等待发送到 Kafka 的事件。 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {
  @Id
  @Column(name = "event_id", length = 100)
  private String eventId;

  @Column(name = "order_id", nullable = false, length = 100)
  private String orderId;

  @Column(nullable = false, length = 200)
  private String topic;

  @Lob
  @Column(nullable = false)
  private String payload;

  @JdbcTypeCode(SqlTypes.TIMESTAMP)
  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @JdbcTypeCode(SqlTypes.TIMESTAMP)
  @Column(name = "sent_at")
  private Instant sentAt;

  @Column(name = "partition_id")
  private Integer partition;

  @Column(name = "offset_id")
  private Long offset;

  protected OutboxEvent() {}

  public OutboxEvent(
      String eventId, String orderId, String topic, String payload, Instant createdAt) {
    this.eventId = eventId;
    this.orderId = orderId;
    this.topic = topic;
    this.payload = payload;
    this.createdAt = createdAt;
  }

  public void markSent(int partition, long offset, Instant sentAt) {
    this.partition = partition;
    this.offset = offset;
    this.sentAt = sentAt;
  }

  public String getEventId() { return eventId; }
  public String getOrderId() { return orderId; }
  public String getTopic() { return topic; }
  public String getPayload() { return payload; }
  public Instant getCreatedAt() { return createdAt; }
  public Instant getSentAt() { return sentAt; }
  public Integer getPartition() { return partition; }
  public Long getOffset() { return offset; }
}
