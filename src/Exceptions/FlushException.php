<?php

declare(strict_types=1);

namespace VladislavPanda\KafkaAdapter\Exceptions;

use RuntimeException;
use Throwable;

class FlushException extends RuntimeException
{
    public function __construct(string $message = '', int $code = 0, ?Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
