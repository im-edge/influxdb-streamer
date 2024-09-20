<?php

namespace IMEdge\InfluxDbStreamer;

use Amp\Http\Client\HttpClient;
use Amp\Http\Client\HttpClientBuilder;
use Amp\Redis\RedisClient;
use IMEdge\Json\JsonString;
use IMEdge\InfluxDbStreamer\InfluxDb\InfluxDbWriterV1;
use IMEdge\InfluxDbStreamer\InfluxDb\LineProtocol;
use IMEdge\Metrics\Measurement;
use IMEdge\RedisUtils\RedisResult;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;

use function Amp\delay;
use function Amp\Redis\createRedisClient;

class InfluxDbStreamer
{
    protected const INFLUXDB_CHUNK_SIZE = 10_000;
    protected const INTERNAL_METRICS_STREAM = 'internalMetrics';
    protected const INTERNAL_METRICS_STREAM_POSITION = self::INTERNAL_METRICS_STREAM . '/position';
    protected const INITIAL_STREAM_POSITION = '0-0';

    protected RedisClient $redis;
    protected string $position;
    protected array $buffer = [];
    protected HttpClient $httpClient;
    protected bool $processing = false;
    protected ?string $currentError = null;

    public function __construct(
        protected readonly string $redisSocket,
        protected readonly InfluxDbWriterV1 $writer,
        protected readonly LoggerInterface $logger,
    ) {
        $this->httpClient = HttpClientBuilder::buildDefault();
    }

    public function run(): void
    {
        $this->redis = createRedisClient('unix://' . $this->redisSocket);
        $this->position = $this->redis->execute('get', self::INTERNAL_METRICS_STREAM_POSITION)
            ?? self::INITIAL_STREAM_POSITION;

        while (true) {
            $this->fetchNextBatch();
        }
    }

    protected function fetchNextBatch(): void
    {
        if (count($this->buffer) > 1_000_0000) {
            delay(0.1);
            return;
        }
        $result = $this->redis->execute(
            'XREAD',
            'COUNT',
            1_000,
            'BLOCK',
            10_000,
            'STREAMS',
            self::INTERNAL_METRICS_STREAM,
            $this->position
        );
        if ($result) {
            foreach ($result as $streamResult) {
                if ($streamResult[0] === self::INTERNAL_METRICS_STREAM) {
                    $this->processStreamResult($streamResult[1]);
                }
            }
        }
    }

    protected function processStreamResult($result): void
    {
        foreach ($result as $row) {
            $this->position = $row[0];
            $value = RedisResult::toArray($row[1]);
            $measurement = Measurement::fromSerialization(JsonString::decode($value['measurement']));
            $tags = [
                'pollingNode' => $measurement->ci->hostname,
                'instance' => $measurement->ci->instance,
            ];
            $fields = [];
            foreach ($measurement->getMetrics() as $metric) {
                $fields[$metric->label] = $metric->value;
            }
            $this->buffer[] = LineProtocol::renderMeasurement(
                $measurement->ci->subject,
                $tags,
                $fields,
                $measurement->getTimestamp()
            );
        }

        if (! $this->processing) {
            $this->processing = true;
            EventLoop::delay(0.1, $this->shipBuffer(...));
        }
    }

    protected function shipBuffer(): void
    {
        while (! empty($this->buffer)) {
            $send = array_slice($this->buffer, 0, self::INFLUXDB_CHUNK_SIZE);
            $this->buffer = array_slice($this->buffer, count($send) + 1);
            $body = implode('', $send);

            $failing = true;
            while ($failing) {
                try {
                    $start = microtime(true);
                    $this->writer->write($body);
                    $duration = (microtime(true) - $start) * 1000;
                    $this->logger->debug(sprintf(
                        'Shipped %d measurements to InfluxDB in %.2Fms',
                        count($send),
                        $duration
                    ));
                    $this->confirmPosition();
                    $failing = false;
                } catch (\Exception $e) {
                    $delay = 5;
                    $this->logErrorOnce(sprintf(
                        'Writing metrics failed: %s, will retry every %ds',
                        $e->getMessage(),
                        $delay
                    ));
                    delay($delay);
                }
            }
        }

        $this->processing = false;
    }

    protected function logErrorOnce(string $message): void
    {
        if ($message !== $this->currentError) {
            $this->logger->error($message);
            $this->currentError = $message;
        }
    }

    protected function confirmPosition(): void
    {
        $this->redis->set(self::INTERNAL_METRICS_STREAM_POSITION, $this->position);
    }
}
