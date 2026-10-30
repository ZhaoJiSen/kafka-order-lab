<?php

return [
    'brokers' => env('KAFKA_BROKERS', 'kafka:9092'),
    'created_topic' => 'demo.orders.created',
    'reviewed_topic' => 'demo.orders.reviewed',
    'result_group' => 'demo-php-order-results',
];
