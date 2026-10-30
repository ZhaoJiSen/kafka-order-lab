<?php

namespace App\Services;

use RdKafka\Conf;
use RdKafka\Message;
use RdKafka\Producer;
use RdKafka\TopicConf;
use RuntimeException;

class KafkaProducer
{
    public function publish(string $topicName, string $key, string $payload): array
    {
        $report = null;
        $conf = new Conf;
        $conf->set('metadata.broker.list', config('demo.brokers'));
        $conf->set('client.id', 'php-orders-producer');
        $conf->set('enable.idempotence', 'true');
        $conf->set('acks', 'all');
        $conf->set('message.timeout.ms', '10000');
        $conf->setDrMsgCb(function (Producer $_producer, Message $message) use (&$report) {
            $report = ['error' => $message->err, 'partition' => $message->partition, 'offset' => $message->offset];
        });

        // 1. 创建 rdkafka 扩展提供的生产者客户端
        $producer = new Producer($conf);

        // 2. 获取要写入的 Topic 对象；这里并不是创建 Kafka 服务端的 Topic
        $topicConf = new TopicConf;
        $topicConf->set('produce.offset.report', 'true');
        $topic = $producer->newTopic($topicName, $topicConf);

        // UA 让客户端根据 Key 选择分区；Kafka 在追加保存时分配 Offset。
        // 把消息放进客户端发送队列，交给客户端发送。$payload：要保存的消息内容；$key：这里传入的是订单编号，客户端用它计算分区
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload, $key);

        // 等待发送完成，最多等待 12 秒，并配合后面的代码检查发送结果
        $result = $producer->flush(12000);
        if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR || ! $report || $report['error'] !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw new RuntimeException('Kafka 尚未确认这条消息，Outbox 会保留并重试。');
        }

        return ['topic' => $topicName, 'key' => $key, 'partition' => $report['partition'], 'offset' => $report['offset']];
    }
}
