package demo.order.service;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ReviewRuleTest {
  private final ReviewRule rule = new ReviewRule();

  @Test
  void approvalIncludesTheExactLimit() {
    assertEquals("CONFIRMED", rule.decide(100000).status());
  }

  @Test
  void rejectsOneCentAboveLimit() {
    assertEquals("REJECTED", rule.decide(100001).status());
  }

  @Test
  void refusesInvalidAmounts() {
    assertThrows(IllegalArgumentException.class, () -> rule.decide(0));
  }
}
