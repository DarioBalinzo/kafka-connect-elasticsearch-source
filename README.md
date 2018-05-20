# kafka-connect-elasticsearch-source
Kafka Connect Elasticsearch Source: fetch data from elasting indices using scroll API, fetch only new data using an incremental / temporal field, dynamic schema generation with support of nested objects/ arrays.

## Elasticsearch Configuration

``es.host``
  ElasticSearch host

  * Type: string
  * Importance: high
  * Dependents: ``index.prefix``

``es.port``
  ElasticSearch port

  * Type: string
  * Importance: high
  * Dependents: ``index.prefix``

``es.user``
  Elasticsearch username

  * Type: string
  * Default: null
  * Importance: high

``es.password``
  Elasticsearch password

  * Type: password
  * Default: null
  * Importance: high

``connection.attempts``
  Maximum number of attempts to retrieve a valid Elasticsearch connection.

  * Type: int
  * Default: 3
  * Importance: low

``connection.backoff.ms``
  Backoff time in milliseconds between connection attempts.

  * Type: long
  * Default: 10000
  * Importance: low

``index.prefix``
  List of indices to include in copying.

  * Type: list
  * Default: ""
  * Importance: medium


## Connector Configuration

``poll.interval.ms``
  Frequency in ms to poll for new data in each index.

  * Type: int
  * Default: 5000
  * Importance: high

``batch.max.rows``
  Maximum number of documents to include in a single batch when polling for new data.

  * Type: int
  * Default: 10000
  * Importance: low

``topic.prefix``
  Prefix to prepend to index names to generate the name of the Kafka topic to publish data

  * Type: string
  * Importance: high
