<?php

declare(strict_types=1);

namespace VladislavPanda\KafkaAdapter;

use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Message;

final readonly class Consumer
{
    private Configuration $configuration;

    private RdKafkaConsumer $rdKafkaConsumer;

    public function setConfiguration(array $appliedConfigs): Consumer
    {
        $this->configuration = new Configuration($appliedConfigs);
        $this->rdKafkaConsumer = new RdKafkaConsumer($this->configuration->getConf());

        return $this;
    }

    public function setTopic(string $topic): Consumer
    {
        $this->rdKafkaConsumer->subscribe((array) $topic);

        return $this;
    }

    public function consume(int $timeoutMs = 5000): Message
    {
        return $this->rdKafkaConsumer->consume($timeoutMs);
    }
}