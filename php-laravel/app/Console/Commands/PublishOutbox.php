<?php

namespace App\Console\Commands;

use App\Services\KafkaProducer;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class PublishOutbox extends Command
{
    // 它定义了一个 Artisan 命令 php artisan demo:publish-outbox
    // 运行后，Laravel 会执行这个类的 handle() 方法
    protected $signature = 'demo:publish-outbox {--once}';

    protected $description = '把 PHP 数据库中的待发送事件发布到 Kafka';

    public function handle(KafkaProducer $producer): int
    {
        $running = true;
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, function () use (&$running) {
            $running = false;
        });
        pcntl_signal(SIGINT, function () use (&$running) {
            $running = false;
        });

        // 外层循环每隔半秒再次检查，所以新订单的事件保存进来后，发送进程就能发现它
        do {
            foreach (
                // 查询待发送事件表 → 找出发送时间为空的记录 → 按创建时间排列 → 最多取 20 条
                DB::table('outbox_events')
                    ->whereNull('sent_at')
                    ->orderBy('created_at')
                    ->limit(20)
                    ->get() as $event
            ) {
                try {
                    $meta = $producer->publish($event->topic, $event->order_id, $event->payload);

                    // 只有 publish() 成功返回，才会执行这个更新
                    DB::table('outbox_events')->where('event_id', $event->event_id)->update([
                        'sent_at' => now(),
                        'kafka_partition' => $meta['partition'],
                        'kafka_offset' => $meta['offset'],
                        'attempts' => DB::raw('attempts + 1'),
                        'last_error' => null
                    ]);
                    Log::info('PHP → Kafka：OrderCreated 已确认写入', ['eventId' => $event->event_id, ...$meta]);
                } catch (\Throwable $error) {
                    DB::table('outbox_events')->where('event_id', $event->event_id)->update([
                        'attempts' => DB::raw('attempts + 1'),
                        'last_error' => $error->getMessage()
                    ]);
                    Log::warning('PHP 发送失败，保留事件等待重试', ['error' => $error->getMessage()]);
                }
            }
            if ($this->option('once')) {
                break;
            }
            usleep(500000);
        } while ($running);

        return self::SUCCESS;
    }
}
