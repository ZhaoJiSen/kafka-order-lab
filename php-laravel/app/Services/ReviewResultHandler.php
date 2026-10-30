<?php

namespace App\Services;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;
use RdKafka\Message;

class ReviewResultHandler
{
    public function handle(Message $message): bool
    {
        $event = json_decode($message->payload, true, flags: JSON_THROW_ON_ERROR);
        Validator::make($event, [
            'eventId' => ['required', 'string', 'max:100'],
            'eventType' => ['required', 'in:OrderReviewed'],
            'version' => ['required', 'in:1'],
            'source' => ['required', 'in:java-review'],
            'correlationId' => ['required', 'string'],
            'occurredAt' => ['required', 'date'],
            'data.orderId' => ['required', 'string'],
            'data.status' => ['required', 'in:CONFIRMED,REJECTED'],
            'data.reason' => ['required', 'string', 'max:500'],
            'data.inputRecord.topic' => ['required', 'in:demo.orders.created'],
            'data.inputRecord.partition' => ['required', 'integer', 'min:0'],
            'data.inputRecord.offset' => ['required', 'integer', 'min:0'],
        ])->validate();
        $orderId = $event['data']['orderId'];
        if ($message->key !== $orderId || $event['correlationId'] !== $orderId) {
            throw new \RuntimeException('消息 Key、correlationId 与订单编号不一致。');
        }

        return DB::transaction(function () use ($message, $event, $orderId) {
            // 放置处理重复订单
            if (DB::table('message_receipts')->where('event_id', $event['eventId'])->exists()) {
                return false;
            }

            $order = DB::table('orders')->where('id', $orderId)->first();

            if (! $order) {
                throw new \RuntimeException('审核结果对应的订单不存在。');
            }

            // 只处理状态为 PENDING_REVIEW
            // 把订单更新为 Java 返回的审核结果
            if ($order->status === 'PENDING_REVIEW') {
                DB::table('orders')->where('id', $orderId)->update([
                    'status' => $event['data']['status'],
                    'review_reason' => $event['data']['reason'],
                    'updated_at' => now()
                ]);
            }

            // 记录 PHP 已处理过哪些审核结果
            DB::table('message_receipts')->insert([
                'event_id' => $event['eventId'],
                'order_id' => $orderId,
                'topic' => $message->topic_name,
                'kafka_partition' => $message->partition,
                'kafka_offset' => $message->offset,
                'message_key' => $message->key,
                'payload' => $message->payload,
                'handled_at' => now()
            ]);

            return true;
        }, 5);
    }
}
