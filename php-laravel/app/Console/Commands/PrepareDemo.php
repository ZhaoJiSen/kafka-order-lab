<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;

class PrepareDemo extends Command
{
    protected $signature = 'demo:prepare';

    protected $description = '准备本 Demo 的持久化 SQLite 数据库';

    public function handle(): int
    {
        $path = config('database.connections.sqlite.database');
        if (! is_dir(dirname($path))) {
            mkdir(dirname($path), 0775, true);
        }
        if (! file_exists($path)) {
            touch($path);
        }

        return $this->call('migrate', ['--force' => true]);
    }
}
