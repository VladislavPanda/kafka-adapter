<?php

declare(strict_types=1);

namespace VladislavPanda\KafkaAdapter\Exceptions;

use RuntimeException;
use Throwable;

class ProduceException extends RuntimeException
{
    public function __construct(
        string $message = 'Failed to queue message in Kafka buffer: ',
        int $code = 0,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, $code, $previous);
    }
}
