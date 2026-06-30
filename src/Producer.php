<?php

declare(strict_types=1);

namespace VladislavPanda\KafkaAdapter;

use RdKafka\Producer as RdKafkaProducer;
use RdKafka\Topic;
use Throwable;
use VladislavPanda\KafkaAdapter\Exceptions\FlushException;
use VladislavPanda\KafkaAdapter\Exceptions\ProduceException;

final class Producer
{
    private const DEFAULT_MSG_FLAGS = 0;

    private const DEFAULT_POLL_TIMEOUT_MS = 0;

    private Configuration $configuration;

    private RdKafkaProducer $rdKafkaProducer;

    private Topic $topic;

    private string $message;

    private ?string $messageKey = null;

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

    public function setMessageKey(string $messageKey): Producer
    {
        $this->messageKey = $messageKey;

        return $this;
    }

    public function produce(int $timeoutMs = 1000): void
    {
        try {
            $this->topic->produce(RD_KAFKA_PARTITION_UA, self::DEFAULT_MSG_FLAGS, $this->message, $this->messageKey);
        } catch (Throwable $exception) {
            throw new ProduceException(previous: $exception);
        }

        $this->rdKafkaProducer->poll(self::DEFAULT_POLL_TIMEOUT_MS);

        $result = $this->rdKafkaProducer->flush($timeoutMs);

        if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw new FlushException(
                sprintf("Kafka delivery failed. Error code: %d. Reason: %s", $result, rd_kafka_err2str($result)),
            );
        }
    }
}
