package demo.order.messaging;

import demo.order.service.ReviewService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedListener {
  private final EventCodec codec;
  private final ReviewService reviews;

  public OrderCreatedListener(EventCodec codec, ReviewService reviews) {
    this.codec = codec;
    this.reviews = reviews;
  }

  // Spring Kafka 根据 @KafkaListener 启动监听容器，由容器里的消费者线程读取消息，再调用这个方法
  @KafkaListener(topics = "${demo.created-topic}")
  public void receive(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) throws Exception {
    reviews.handle(codec.read(record), record);
    // Spring 的事务代理已经提交数据库后，才走到这里提交下一消费位置。
    acknowledgment.acknowledge();
  }
}
