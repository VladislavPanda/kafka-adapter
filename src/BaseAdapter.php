<?php

declare(strict_types=1);

namespace VladislavPanda\KafkaAdapter;

final readonly class BaseAdapter
{
    public static function producer(): Producer
    {
        return new Producer();
    }

    public static function consumer(): Consumer
    {
        return new Consumer();
    }
}
