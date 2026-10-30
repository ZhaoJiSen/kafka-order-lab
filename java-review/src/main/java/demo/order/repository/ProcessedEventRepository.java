package demo.order.repository;

import demo.order.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
  long countByOrderId(String orderId);
}
