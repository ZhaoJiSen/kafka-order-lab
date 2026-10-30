package demo.order.messaging.event;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderCreatedEvent(
        @NotBlank @Size(max = 100) String eventId,
        @NotBlank String eventType,
        int version,
        @NotBlank String source,
        @NotNull Instant occurredAt,
        @NotBlank String correlationId,
        @NotNull @Valid Data data) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Data(
            @NotBlank @Size(max = 100) String orderId,
            @NotBlank @Size(max = 100) String product,
            @Min(1) @Max(1000) int quantity,
            @Min(1) @Max(10000000) long unitPriceCents,
            @Positive long totalCents) {
    }
}
