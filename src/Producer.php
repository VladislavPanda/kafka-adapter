<?php

declare(strict_types=1);

namespace VladislavPanda\KafkaAdapter;

use RdKafka\Producer as RdKafkaProducer;
use RdKafka\Topic;

final class Producer
{
    private Configuration $configuration;

    private RdKafkaProducer $rdKafkaProducer;

    private Topic $topic;

    private string $message;

    public function setConfiguration(array $appliedConfigs): self
    {
        $this->configuration = new Configuration($appliedConfigs);
        $this->rdKafkaProducer = new RdKafkaProducer($this->configuration->getConf());

        return $this;
    }

    public function setTopic(string $topic): Producer
    {
        $this->topic = $this->rdKafkaProducer->newTopic($topic);

        return $this;
    }

    public function setMessage(array $message): Producer
    {
        $this->message = json_encode($message, JSON_UNESCAPED_UNICODE);

        return $this;
    }

    public function produce(int $timeoutMs): void
    {
        $this->topic->produce(RD_KAFKA_PARTITION_UA, 0, $this->message);

        $this->rdKafkaProducer->flush($timeoutMs);
    }
}
