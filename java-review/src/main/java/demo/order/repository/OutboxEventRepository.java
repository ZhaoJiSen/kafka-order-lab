package demo.order.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import demo.order.entity.OutboxEvent;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

  /**
   * 按创建时间，最多取 20 条尚未发送的事件。
   * Spring Data JPA 实现
   *
   * findTop20 查找，最多取 20 条
   * By 后面开始描述查询条件
   * SentAtIsNull sentAt 字段为空
   * OrderBy 后面开始描述排序
   * CreatedAtAsc 创建时间升序
   * EventIdAsc 创建时间相同时，按事件编号升序
   *
   */
  List<OutboxEvent> findTop20BySentAtIsNullOrderByCreatedAtAscEventIdAsc();

  long countByOrderId(String orderId);

  long countByOrderIdAndSentAtIsNotNull(String orderId);
}
