package demo.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OrderReviewApplication {
  public static void main(String[] args) {
    SpringApplication.run(OrderReviewApplication.class, args);
  }
}
