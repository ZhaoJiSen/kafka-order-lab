<?php

namespace App\Http\Controllers;

use App\Http\Requests\CreateOrderRequest;
use App\Services\OrderService;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\Validator;

class OrderController extends Controller
{
    public function __construct(private readonly OrderService $orders) {}

    public function index(): JsonResponse
    {
        return response()->json(['orders' => $this->orders->all()]);
    }

    public function show(string $order): JsonResponse
    {
        return response()->json($this->orders->detail($order));
    }

    public function store(CreateOrderRequest $request): JsonResponse
    {
        $key = $request->header('Idempotency-Key');
        Validator::make(['key' => $key], ['key' => ['required', 'string', 'max:100']])->validate();

        // 执行到这里，还没有向 Kafka 发送消息
        return response()->json($this->orders->create($request->validated(), $key), 202);
    }
}
