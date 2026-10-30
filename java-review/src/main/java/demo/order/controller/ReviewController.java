package demo.order.controller;

import demo.order.entity.Review;
import demo.order.repository.OutboxEventRepository;
import demo.order.repository.ProcessedEventRepository;
import demo.order.repository.ReviewRepository;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/reviews")
public class ReviewController {
  private final ReviewRepository reviews;
  private final ProcessedEventRepository processedEvents;
  private final OutboxEventRepository outbox;

  public ReviewController(
      ReviewRepository reviews,
      ProcessedEventRepository processedEvents,
      OutboxEventRepository outbox) {
    this.reviews = reviews;
    this.processedEvents = processedEvents;
    this.outbox = outbox;
  }

  @GetMapping("/{orderId}")
  public ReviewResponse show(@PathVariable String orderId) {
    var review = reviews.findById(orderId)
        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "审核记录不存在"));
    return new ReviewResponse(
        ReviewDetails.from(review),
        processedEvents.countByOrderId(orderId),
        outbox.countByOrderId(orderId),
        outbox.countByOrderIdAndSentAtIsNotNull(orderId));
  }

  public record ReviewResponse(
      ReviewDetails review, long processedEvents, long resultEvents, long publishedResults) {}

  // 接口返回专用对象，保持原来的 JSON 字段，不直接暴露数据库实体。
  public record ReviewDetails(
      String orderId, String status, String reason, long totalCents, String requestEventId) {
    static ReviewDetails from(Review review) {
      return new ReviewDetails(review.getOrderId(), review.getStatus(), review.getReason(),
          review.getTotalCents(), review.getEventId());
    }
  }
}
