import 'dotenv/config';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
export const root = fileURLToPath(new URL('../../', import.meta.url));
export const config = {
  php: process.env.PHP_API_URL ?? 'http://127.0.0.1:8000',
  java: process.env.JAVA_API_URL ?? 'http://127.0.0.1:18081',
  brokers: (process.env.KAFKA_BROKERS ?? '127.0.0.1:19092').split(','),
  port: Number(process.env.OBSERVER_PORT ?? 3010),
  mode: process.env.PHP_RUNTIME ?? 'native',
  javaMode: process.env.JAVA_RUNTIME ?? 'native',
  local: resolve(root, 'frontend/.local'),
  phpLog: resolve(root, 'php-laravel/storage/logs/laravel.log'),
  javaLog: resolve(root, 'java-review/.local/logs/java-review.log'),
};
