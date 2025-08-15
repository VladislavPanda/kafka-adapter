<?php

declare(strict_types=1);

namespace VladislavPanda\KafkaAdapter;

use RdKafka\Conf;

final readonly class Configuration
{
    private Conf $conf;

    public function __construct(array $appliedConfigs = [])
    {
        $conf = new Conf();

        foreach ($appliedConfigs as $configKey => $configValue) {
            $conf->set($configKey, $configValue);
        }

        $this->conf = $conf;
    }

    public function getConf(): Conf
    {
        return $this->conf;
    }
}