package demo.order.entity;

import demo.order.messaging.event.OrderCreatedEvent;
import demo.order.service.ReviewRule;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** 一笔订单的审核结果，对应 reviews 表的一行。 */
@Entity
@Table(name = "reviews")
public class Review {
  @Id
  @Column(name = "order_id", length = 100)
  private String orderId;

  @Column(name = "event_id", nullable = false, length = 100)
  private String eventId;

  @Column(nullable = false, length = 30)
  private String status;

  @Column(nullable = false, length = 500)
  private String reason;

  @Column(name = "total_cents", nullable = false)
  private long totalCents;

  @JdbcTypeCode(SqlTypes.TIMESTAMP)
  @Column(name = "reviewed_at", nullable = false)
  private Instant reviewedAt;

  protected Review() {} // JPA 加载数据库记录时使用。

  public Review(OrderCreatedEvent event, ReviewRule.Decision decision, Instant reviewedAt) {
    this.orderId = event.data().orderId();
    this.eventId = event.eventId();
    this.status = decision.status();
    this.reason = decision.reason();
    this.totalCents = event.data().totalCents();
    this.reviewedAt = reviewedAt;
  }

  public String getOrderId() { return orderId; }
  public String getEventId() { return eventId; }
  public String getStatus() { return status; }
  public String getReason() { return reason; }
  public long getTotalCents() { return totalCents; }
  public Instant getReviewedAt() { return reviewedAt; }
}
