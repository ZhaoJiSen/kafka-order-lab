package demo.order.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaFailureConfig {
  @Bean
  public DefaultErrorHandler kafkaErrorHandler() {
    // 教学中保留失败记录重试，不在重试耗尽后悄悄跳过。生产环境还要设计死信和告警。
    return new DefaultErrorHandler(new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
  }
}
