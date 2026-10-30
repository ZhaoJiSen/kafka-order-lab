<?php

namespace App\Console\Commands;

use App\Services\ReviewResultHandler;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

class ConsumeResults extends Command
{
    protected $signature = 'demo:consume-results';

    protected $description = '消费 Java 的审核结果，更新 PHP 订单后再提交进度';

    public function handle(ReviewResultHandler $handler): int
    {
        $conf = new Conf;
        $conf->set('metadata.broker.list', config('demo.brokers'));
        $conf->set('group.id', config('demo.result_group'));
        $conf->set('client.id', 'php-order-results');
        $conf->set('auto.offset.reset', 'earliest');
        $conf->set('enable.auto.commit', 'false');
        $conf->set('enable.auto.offset.store', 'false');
        $consumer = new KafkaConsumer($conf);
        $consumer->subscribe([config('demo.reviewed_topic')]);
        $running = true;
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, function () use (&$running) {
            $running = false;
        });
        pcntl_signal(SIGINT, function () use (&$running) {
            $running = false;
        });
        try {
            while ($running) {
                $message = $consumer->consume(1000);
                if (in_array($message->err, [RD_KAFKA_RESP_ERR__TIMED_OUT, RD_KAFKA_RESP_ERR__PARTITION_EOF], true)) {
                    continue;
                }
                if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                    throw new \RuntimeException($message->errstr());
                }
                $changed = $handler->handle($message);
                // 只有数据库事务完成后才提交下一位置；失败时退出并由容器重启重读。
                $consumer->commit($message);
                Log::info($changed ? 'Kafka → PHP：订单结果已保存并提交进度' : 'Kafka → PHP：识别重复事件，跳过业务修改',
                    ['topic' => $message->topic_name, 'partition' => $message->partition, 'offset' => $message->offset,
                        'nextOffset' => $message->offset + 1, 'key' => $message->key]);
            }
        } finally {
            $consumer->close();
        }

        return self::SUCCESS;
    }
}
