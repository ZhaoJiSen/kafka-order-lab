package demo.order.messaging;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import demo.order.messaging.event.OrderCreatedEvent;
import jakarta.validation.Validation;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class EventCodecTest {
  private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

  private OrderCreatedEvent event(long total) {
    return new OrderCreatedEvent(
        "evt-test",
        "OrderCreated",
        1,
        "php-orders",
        Instant.now(),
        "ord-test",
        new OrderCreatedEvent.Data("ord-test", "键盘", 2, 19900, total));
  }

  @Test
  void acceptsTheSharedPhpContract() throws Exception {
    try (var factory = Validation.buildDefaultValidatorFactory()) {
      var codec = new EventCodec(mapper, factory.getValidator());
      assertEquals(
          "ord-test",
          codec
              .read(
                  new ConsumerRecord<>(
                      "demo.orders.created",
                      0,
                      0,
                      "ord-test",
                      mapper.writeValueAsString(event(39800))))
              .data()
              .orderId());
    }
  }

  @Test
  void rejectsAnOrderRoutedWithAnotherOrdersKey() throws Exception {
    try (var factory = Validation.buildDefaultValidatorFactory()) {
      var codec = new EventCodec(mapper, factory.getValidator());
      var record =
          new ConsumerRecord<>(
              "demo.orders.created", 0, 0, "ord-other", mapper.writeValueAsString(event(39800)));
      assertThrows(IllegalArgumentException.class, () -> codec.read(record));
    }
  }

  @Test
  void rejectsInconsistentAmounts() throws Exception {
    try (var factory = Validation.buildDefaultValidatorFactory()) {
      var codec = new EventCodec(mapper, factory.getValidator());
      var record =
          new ConsumerRecord<>(
              "demo.orders.created", 0, 0, "ord-test", mapper.writeValueAsString(event(1)));
      assertThrows(IllegalArgumentException.class, () -> codec.read(record));
    }
  }
}
