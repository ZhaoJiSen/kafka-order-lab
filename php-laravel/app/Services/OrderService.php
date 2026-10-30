<?php

namespace App\Services;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Symfony\Component\HttpKernel\Exception\ConflictHttpException;

class OrderService
{
    // $requestKey 识别是不是同一次下单请求
    // 假设第一次下单已经成功，但网络断开，页面没收到响应。页面重试时，沿用原来的 Key，后端就能返回已有订单，避免创建第二笔
    public function create(array $input, string $requestKey): array
    {
        // 订单和待发送消息在同一个数据库事务中保存；Kafka 发送交给独立进程。
        return DB::transaction(function () use ($input, $requestKey) {
            $data = [
                'product' => $input['product'],
                'quantity' => (int) $input['quantity'],
                'unitPriceCents' => (int) $input['unitPriceCents']
            ];

            // 判断下单内容有没有变化
            $fingerprint = hash('sha256', json_encode($data, JSON_THROW_ON_ERROR));
            $existing = DB::table('orders')->where('request_key', $requestKey)->first();

            if ($existing) {
                if ($existing->request_fingerprint !== $fingerprint) {
                    throw new ConflictHttpException('同一个 Idempotency-Key 对应了不同的下单内容。');
                }

                return ['orderId' => $existing->id, 'status' => $existing->status, 'reused' => true];
            }

            // 创建和审核完成”两件不同的事，因此有各自的事件编号
            // Java 创建审核结果事件时，会生成新的 eventId，并沿用原来的 orderId
            $orderId = 'ord-' . Str::uuid();
            $eventId = 'evt-' . Str::uuid();
            $total = $data['quantity'] * $data['unitPriceCents'];

            DB::table('orders')->insert([
                'id' => $orderId,
                'request_key' => $requestKey,
                'request_fingerprint' => $fingerprint,
                'product' => $data['product'],
                'quantity' => $data['quantity'],
                'unit_price_cents' => $data['unitPriceCents'],
                'total_cents' => $total,
                'status' => 'PENDING_REVIEW',
                'created_at' => now(),
                'updated_at' => now()
            ]);

            $event = [
                'eventId' => $eventId,
                'eventType' => 'OrderCreated',
                'version' => 1,
                'source' => 'php-orders',
                'occurredAt' => now()->toISOString(),
                'correlationId' => $orderId,
                'data' => ['orderId' => $orderId, ...$data, 'totalCents' => $total]
            ];

            // 此时订单已经存进数据库，outbox_events 表中也有一条待发送事件，接下来就是谁把事件发送到 Kafka 中
            DB::table('outbox_events')->insert([
                'event_id' => $eventId,
                'order_id' => $orderId,
                'topic' => config('demo.created_topic'),
                'payload' => json_encode($event, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                'created_at' => now()
            ]);

            return ['orderId' => $orderId, 'status' => 'PENDING_REVIEW', 'reused' => false];
        }, 5);
    }

    public function all(): array
    {
        return DB::table('orders')->orderByDesc('created_at')->limit(30)->get()
            ->map(fn($order) => $this->detail($order->id))->all();
    }

    public function detail(string $id): array
    {
        $order = DB::table('orders')->where('id', $id)->first();
        abort_unless($order, 404, '订单不存在。');
        $sent = DB::table('outbox_events')->where('order_id', $id)->first();
        $received = DB::table('message_receipts')->where('order_id', $id)->orderBy('handled_at')->first();

        return [
            'orderId' => $order->id,
            'product' => $order->product,
            'quantity' => $order->quantity,
            'totalCents' => $order->total_cents,
            'status' => $order->status,
            'reason' => $order->review_reason,
            'createdAt' => $order->created_at,
            'outbound' => $sent ? [
                'event' => json_decode($sent->payload, true),
                'topic' => $sent->topic,
                'key' => $id,
                'partition' => $sent->kafka_partition,
                'offset' => $sent->kafka_offset,
                'sentAt' => $sent->sent_at,
                'attempts' => $sent->attempts,
                'lastError' => $sent->last_error
            ] : null,
            'inbound' => $received ? [
                'event' => json_decode($received->payload, true),
                'topic' => $received->topic,
                'key' => $received->message_key,
                'partition' => $received->kafka_partition,
                'offset' => $received->kafka_offset,
                'handledAt' => $received->handled_at
            ] : null
        ];
    }
}
