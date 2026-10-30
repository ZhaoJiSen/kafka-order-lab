<?php

namespace App\Console\Commands;

use App\Services\KafkaProducer;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class ReplayCreated extends Command
{
    protected $signature = 'demo:replay-created {orderId}';

    protected $description = '再次发送原始创建事件，观察 Java 按 eventId 去重';

    public function handle(KafkaProducer $producer): int
    {
        $event = DB::table('outbox_events')->where('order_id', $this->argument('orderId'))->first();
        if (! $event) {
            $this->error('订单不存在。');

            return self::FAILURE;
        }
        $this->line(json_encode($producer->publish($event->topic, $event->order_id, $event->payload), JSON_UNESCAPED_UNICODE));

        return self::SUCCESS;
    }
}
