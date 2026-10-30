package demo.order.integration;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import demo.order.controller.ReviewController;
import demo.order.entity.OutboxEvent;
import demo.order.entity.Review;
import demo.order.messaging.OutboxPublisher;
import demo.order.messaging.event.OrderCreatedEvent;
import demo.order.messaging.event.OrderReviewedEvent;
import demo.order.repository.OutboxEventRepository;
import demo.order.repository.ProcessedEventRepository;
import demo.order.repository.ReviewRepository;
import demo.order.service.ReviewRule;
import demo.order.service.ReviewService;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

// 独立内存库沿用原 schema.sql，并校验映射；不会连接本机 Kafka 或打开真实订单数据库。
@DataJpaTest(properties = {
    "spring.datasource.url=jdbc:h2:mem:review-orm-test;DB_CLOSE_DELAY=-1",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.jpa.show-sql=false",
    "spring.sql.init.mode=always",
    "logging.file.name=",
    "demo.reviewed-topic=demo.orders.reviewed"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(classes = ReviewPersistenceTest.JpaTestConfig.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class ReviewPersistenceTest {
  @Configuration(proxyBeanMethods = false)
  @EntityScan(basePackageClasses = Review.class)
  @EnableJpaRepositories(basePackageClasses = ReviewRepository.class)
  @Import({ReviewService.class, ReviewRule.class, ReviewController.class, OutboxPublisher.class})
  static class JpaTestConfig {
    @Bean
    ObjectMapper objectMapper() {
      return new ObjectMapper().findAndRegisterModules();
    }
  }

  @Autowired ReviewService service;
  @Autowired ReviewRepository reviews;
  @Autowired ProcessedEventRepository processed;
  @Autowired OutboxEventRepository outbox;
  @Autowired OutboxPublisher publisher;
  @Autowired ReviewController controller;
  @Autowired JdbcTemplate jdbc;
  @MockitoSpyBean ObjectMapper mapper;
  @MockitoBean KafkaTemplate<String, String> kafka;

  @BeforeEach
  void clearOnlyTheTestDatabase() {
    outbox.deleteAll();
    reviews.deleteAll();
    processed.deleteAll();
  }

  private OrderCreatedEvent event(String eventId, long totalCents) {
    return new OrderCreatedEvent(
        eventId, "OrderCreated", 1, "php-orders", Instant.parse("2026-10-27T03:00:00Z"),
        "ord-test", new OrderCreatedEvent.Data("ord-test", "键盘", 1, totalCents, totalCents));
  }

  private ConsumerRecord<String, String> record(OrderCreatedEvent event, long offset)
      throws Exception {
    return new ConsumerRecord<>(
        "demo.orders.created", 2, offset, event.data().orderId(), mapper.writeValueAsString(event));
  }

  private void createReview() throws Exception {
    var event = event("evt-created", 9900);
    service.handle(event, record(event, 7));
  }

  @Test
  void approvalSavesTheReviewSourceLocationAndCompatibleResultTogether() throws Exception {
    createReview();
    assertThat(reviews.findById("ord-test").orElseThrow().getStatus()).isEqualTo("CONFIRMED");
    var source = processed.findById("evt-created").orElseThrow();
    assertThat(source.getTopic()).isEqualTo("demo.orders.created");
    assertThat(source.getPartition()).isEqualTo(2);
    assertThat(source.getOffset()).isEqualTo(7);
    var pending = outbox.findAll().getFirst();
    assertThat(pending.getTopic()).isEqualTo("demo.orders.reviewed");
    assertThat(pending.getSentAt()).isNull();
    var result = mapper.readTree(pending.getPayload());
    assertThat(result.path("eventId").asText()).isEqualTo(pending.getEventId());
    assertThat(result.path("eventType").asText()).isEqualTo("OrderReviewed");
    assertThat(result.path("version").asInt()).isEqualTo(1);
    assertThat(result.path("source").asText()).isEqualTo("java-review");
    assertThat(result.path("correlationId").asText()).isEqualTo("ord-test");
    assertThat(result.path("causationId").asText()).isEqualTo("evt-created");
    assertThat(Instant.parse(result.path("occurredAt").asText())).isNotNull();
    assertThat(result.path("data").path("status").asText()).isEqualTo("CONFIRMED");
    assertThat(result.path("data").path("inputRecord")).isEqualTo(mapper.readTree(
        """
        {"topic":"demo.orders.created","partition":2,"offset":7,"key":"ord-test"}
        """));
  }

  @Test
  void expensiveOrderIsRejected() throws Exception {
    var event = event("evt-expensive", 100001);
    service.handle(event, record(event, 8));
    assertThat(reviews.findById("ord-test").orElseThrow().getStatus()).isEqualTo("REJECTED");
    assertThat(outbox.count()).isEqualTo(1);
  }

  @Test
  void redeliveryKeepsTheOriginalResultAndSourceLocation() throws Exception {
    createReview();
    var resultId = outbox.findAll().getFirst().getEventId();
    var event = event("evt-created", 9900);
    service.handle(event, record(event, 20));
    assertThat(processed.count()).isEqualTo(1);
    assertThat(processed.findById("evt-created").orElseThrow().getOffset()).isEqualTo(7);
    assertThat(reviews.count()).isEqualTo(1);
    assertThat(outbox.findAll()).extracting(OutboxEvent::getEventId).containsExactly(resultId);
  }

  @Test
  void newEventIdCannotReviewTheSameOrderAgain() throws Exception {
    createReview();
    var changed = event("evt-another", 100001);
    service.handle(changed, record(changed, 21));
    assertThat(processed.countByOrderId("ord-test")).isEqualTo(2);
    assertThat(reviews.findById("ord-test").orElseThrow().getStatus()).isEqualTo("CONFIRMED");
    assertThat(outbox.countByOrderId("ord-test")).isEqualTo(1);
  }

  @Test
  void checkedSerializationFailureRollsBackAllBusinessWrites() throws Exception {
    var event = event("evt-serialize-fail", 9900);
    var record = record(event, 22);
    doThrow(new JsonProcessingException("模拟序列化失败") {})
        .when(mapper).writeValueAsString(any(OrderReviewedEvent.class));
    assertThatThrownBy(() -> service.handle(event, record)).isInstanceOf(JsonProcessingException.class);
    assertThat(processed.count()).isZero();
    assertThat(reviews.count()).isZero();
    assertThat(outbox.count()).isZero();
  }

  @Test
  void databaseConstraintFailureRollsBackTheWholeTransaction() throws Exception {
    var tooLong = event("x".repeat(101), 9900);
    assertThatThrownBy(() -> service.handle(tooLong, record(tooLong, 23)))
        .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    assertThat(processed.count()).isZero();
    assertThat(reviews.count()).isZero();
    assertThat(outbox.count()).isZero();
  }

  @Test
  void kafkaAcknowledgmentStoresActualMetadataAndPreservesTheHttpContract() throws Exception {
    createReview();
    var pending = outbox.findAll().getFirst();
    var metadata = new RecordMetadata(new TopicPartition("demo.orders.reviewed", 1), 17, 0, 0, 0, 0);
    when(kafka.send(pending.getTopic(), pending.getOrderId(), pending.getPayload()))
        .thenReturn(CompletableFuture.completedFuture(new SendResult<>(
            new ProducerRecord<>(pending.getTopic(), pending.getOrderId(), pending.getPayload()), metadata)));
    publisher.publishPending();
    var sent = outbox.findById(pending.getEventId()).orElseThrow();
    assertThat(sent.getPartition()).isEqualTo(1);
    assertThat(sent.getOffset()).isEqualTo(17);
    assertThat(sent.getSentAt()).isNotNull();
    assertThat(outbox.findTop20BySentAtIsNullOrderByCreatedAtAscEventIdAsc()).isEmpty();
    var mvc = MockMvcBuilders.standaloneSetup(controller).build();
    mvc.perform(get("/api/reviews/ord-test"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.review.orderId").value("ord-test"))
        .andExpect(jsonPath("$.review.status").value("CONFIRMED"))
        .andExpect(jsonPath("$.review.reason").isString())
        .andExpect(jsonPath("$.review.totalCents").value(9900))
        .andExpect(jsonPath("$.review.requestEventId").value("evt-created"))
        .andExpect(jsonPath("$.processedEvents").value(1))
        .andExpect(jsonPath("$.resultEvents").value(1))
        .andExpect(jsonPath("$.publishedResults").value(1));
    mvc.perform(get("/api/reviews/missing")).andExpect(status().isNotFound());
  }

  @Test
  void failedSendStaysPendingAndCanBeRetried() throws Exception {
    createReview();
    var pending = outbox.findAll().getFirst();
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("模拟 Kafka 不可用")));
    publisher.publishPending();
    var unsent = outbox.findById(pending.getEventId()).orElseThrow();
    assertThat(unsent.getSentAt()).isNull();
    assertThat(unsent.getPartition()).isNull();
    assertThat(unsent.getOffset()).isNull();
    assertThat(outbox.findTop20BySentAtIsNullOrderByCreatedAtAscEventIdAsc()).hasSize(1);
    var metadata = new RecordMetadata(new TopicPartition(pending.getTopic(), 0), 18, 0, 0, 0, 0);
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(new SendResult<>(
            new ProducerRecord<>(pending.getTopic(), pending.getOrderId(), pending.getPayload()), metadata)));
    publisher.publishPending();
    assertThat(outbox.countByOrderIdAndSentAtIsNotNull("ord-test")).isEqualTo(1);
    assertThat(outbox.count()).isEqualTo(1);
  }

  @Test
  void readsTimestampAndClobDataWrittenByThePreviousJdbcImplementation() {
    var timestamp = Instant.parse("2026-10-27T03:46:04Z");
    jdbc.update("INSERT INTO reviews VALUES(?,?,?,?,?,?)", "ord-legacy", "evt-legacy",
        "CONFIRMED", "历史审核", 9900, Timestamp.from(timestamp));
    jdbc.update("INSERT INTO outbox_events(event_id,order_id,topic,payload,created_at) VALUES(?,?,?,?,?)",
        "evt-legacy-result", "ord-legacy", "demo.orders.reviewed", "{\"历史\":true}", Timestamp.from(timestamp));
    assertThat(reviews.findById("ord-legacy").orElseThrow().getReviewedAt()).isEqualTo(timestamp);
    var pending = outbox.findById("evt-legacy-result").orElseThrow();
    assertThat(pending.getCreatedAt()).isEqualTo(timestamp);
    assertThat(pending.getPayload()).isEqualTo("{\"历史\":true}");
    assertThat(pending.getSentAt()).isNull();
  }
}
