<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('orders', function (Blueprint $t) {
            $t->string('id')->primary();
            $t->string('request_key')->unique();
            $t->string('request_fingerprint');
            $t->string('product');
            $t->unsignedInteger('quantity');
            $t->unsignedBigInteger('unit_price_cents');
            $t->unsignedBigInteger('total_cents');
            $t->string('status');
            $t->string('review_reason')->nullable();
            $t->timestamps();
        });
        Schema::create('outbox_events', function (Blueprint $t) {
            $t->string('event_id')->primary();
            $t->string('order_id')->index();
            $t->string('topic');
            $t->text('payload');
            $t->timestamp('created_at');
            $t->timestamp('sent_at')->nullable();
            $t->integer('kafka_partition')->nullable();
            $t->bigInteger('kafka_offset')->nullable();
            $t->unsignedInteger('attempts')->default(0);
            $t->text('last_error')->nullable();
        });
        Schema::create('message_receipts', function (Blueprint $t) {
            $t->string('event_id')->primary();
            $t->string('order_id')->index();
            $t->string('topic');
            $t->integer('kafka_partition');
            $t->bigInteger('kafka_offset');
            $t->string('message_key');
            $t->text('payload');
            $t->timestamp('handled_at');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('message_receipts');
        Schema::dropIfExists('outbox_events');
        Schema::dropIfExists('orders');
    }
};
