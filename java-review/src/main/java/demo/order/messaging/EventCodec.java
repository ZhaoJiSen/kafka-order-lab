package demo.order.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import demo.order.messaging.event.OrderCreatedEvent;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

@Component
public class EventCodec {
  private final ObjectMapper mapper;
  private final Validator validator;

  public EventCodec(ObjectMapper mapper, Validator validator) {
    this.mapper = mapper;
    this.validator = validator;
  }

  public OrderCreatedEvent read(ConsumerRecord<String, String> record) throws Exception {
    var event = mapper.readValue(record.value(), OrderCreatedEvent.class);
    if (event == null || !validator.validate(event).isEmpty())
      throw new IllegalArgumentException("订单事件字段不完整");
    if (!"OrderCreated".equals(event.eventType())
        || event.version() != 1
        || !"php-orders".equals(event.source()))
      throw new IllegalArgumentException("不支持的订单事件类型、来源或版本");
    if (!event.data().orderId().equals(record.key()) || !event.correlationId().equals(record.key()))
      throw new IllegalArgumentException("订单编号与消息 Key 不一致");
    if (event.data().totalCents()
        != Math.multiplyExact(event.data().unitPriceCents(), event.data().quantity()))
      throw new IllegalArgumentException("订单金额与数量不一致");
    return event;
  }
}
