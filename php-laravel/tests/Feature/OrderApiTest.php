<?php

namespace Tests\Feature;

use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Tests\TestCase;

class OrderApiTest extends TestCase
{
    use RefreshDatabase;

    public function test_creates_an_order_and_a_durable_message_together(): void
    {
        $this->assertSame(':memory:', config('database.connections.sqlite.database'));
        $response = $this->withHeader('Idempotency-Key', 'test-one')->postJson('/api/orders', [
            'product' => '键盘', 'quantity' => 2, 'unitPriceCents' => 19900]);
        $response->assertStatus(202)->assertJsonPath('status', 'PENDING_REVIEW');
        $this->assertDatabaseHas('orders', ['id' => $response['orderId'], 'total_cents' => 39800]);
        $outbox = DB::table('outbox_events')->sole();
        $this->assertNull($outbox->sent_at);
        $this->assertSame($response['orderId'], json_decode($outbox->payload, true)['data']['orderId']);
    }

    public function test_retrying_the_same_request_does_not_create_two_orders(): void
    {
        $body = ['product' => '键盘', 'quantity' => 2, 'unitPriceCents' => 19900];
        $first = $this->withHeader('Idempotency-Key', 'repeat-one')->postJson('/api/orders', $body);
        $again = $this->withHeader('Idempotency-Key', 'repeat-one')->postJson('/api/orders', $body);
        $again->assertStatus(202)->assertJsonPath('orderId', $first['orderId'])->assertJsonPath('reused', true);
        $this->assertDatabaseCount('orders', 1);
        $this->assertDatabaseCount('outbox_events', 1);
        $this->withHeader('Idempotency-Key', 'repeat-one')->postJson('/api/orders', [...$body, 'quantity' => 3])->assertStatus(409);
    }

    public function test_invalid_amounts_do_not_leave_an_order_or_message(): void
    {
        $this->withHeader('Idempotency-Key', 'invalid')->postJson('/api/orders', [
            'product' => '键盘', 'quantity' => 0, 'unitPriceCents' => 19900])->assertStatus(422);
        $this->assertDatabaseCount('orders', 0);
        $this->assertDatabaseCount('outbox_events', 0);
    }
}
