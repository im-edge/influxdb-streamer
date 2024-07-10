<?php

namespace IMEdge\InfluxDbStreamer\InfluxDb;

use Amp\Http\Client\HttpClient;
use Amp\Http\Client\HttpClientBuilder;
use Amp\Http\Client\Request;
use Ramsey\Uuid\Uuid;

use function base64_encode;
use function gzencode;

class InfluxDbWriterV1
{
    protected const DEFAULT_PRECISION = 's';
    protected const USER_AGENT = 'IMEdge-InfluxDB/0.7';

    protected HttpClient $httpClient;
    protected readonly string $baseUrl;
    /** @var array<string, string> */
    protected array $defaultHeaders;

    public function __construct(
        string $baseUrl,
        protected readonly string $dbName,
        protected readonly ?string $username = null,
        protected readonly ?string $password = null,
    ) {
        $this->baseUrl = ltrim($baseUrl, '/');
        $this->defaultHeaders = $this->prepareDefaultHeaders();
        $this->httpClient = HttpClientBuilder::buildDefault();
    }

    public function write($body): void
    {
        $body = gzencode($body, 6);
        $params = [
            'db'        => $this->dbName,
            'precision' => self::DEFAULT_PRECISION,
        ];
        $headers = [
            'X-Request-Id'     => Uuid::uuid4()->toString(),
            'Content-Encoding' => 'gzip',
            'Content-Length'   => strlen($body),
        ];

        $url = $this->baseUrl . '/write?' . http_build_query($params);
        $request = new Request($url, 'POST', $body);
        $request->setHeaders($headers + $this->defaultHeaders);
        $response = $this->httpClient->request($request);
        if ($response->isSuccessful()) {
            return;
        }

        throw new \Exception(sprintf(
            'Got unexpected %d from InfluxDB: %s',
            $response->getStatus(),
            $response->getReason()
        ));
    }

    protected function prepareDefaultHeaders(): array
    {
        $headers = [
            'User-Agent' => static::USER_AGENT,
        ];
        if ($this->username !== null) {
            $headers['Authorization'] = 'Basic '
                . base64_encode($this->username . ':' . $this->password);
        }

        return $headers;
    }
}
