package demo.order.repository;

import demo.order.entity.Review;
import org.springframework.data.jpa.repository.JpaRepository;

/** Spring 自动实现查询和保存，String 表示主键 orderId 的类型。 */
public interface ReviewRepository extends JpaRepository<Review, String> {}
