# Kafka-connect-elasticsearch-source
[![YourActionName Actions Status](https://github.com/DarioBalinzo/kafka-connect-elasticsearch-source/workflows/Java%20CI%20with%20Maven/badge.svg)](https://github.com/DarioBalinzo/kafka-connect-elasticsearch-source/actions)


Kafka Connect Elasticsearch Source: fetch data from elastic-search and sends it to kafka. The connector fetches only new data using a strictly incremental / temporal field (like a timestamp or an incrementing id).
It supports dynamic schema and nested objects/ arrays.

## Requirements:
- Elasticsearch 6.x and 7.x
- Java >= 8
- Maven

## Bugs or new Ideas?
- Issues tracker: https://github.com/DarioBalinzo/kafka-connect-elasticsearch-source/issues
- Feel free to open an issue to discuss new ideas (or propose new solutions with a PR)
- Do you use this project in production? Would you like new features? This connector will be always free and open source, 
but if you want to support me in the development please consider offering me a coffee (https://www.paypal.me/coffeeDarioBalinzo).

## Installation:
Compile the project with:
```bash
mvn clean package -DskipTests
```

You can also compile and running both unit and integration tests (docker is mandatory) with:
```bash
mvn clean package
```

Copy the jar with dependencies from the target folder into connect classpath (e.g ``/usr/share/java/kafka-connect-elasticsearch`` ) or set ``plugin.path`` parameter appropriately.

## Example
Using kafka connect in distributed way, a sample config file to fetch ``my_awesome_index*`` indices and to produce output topics with ``es_`` prefix:


```json
{       
  "name": "elastic-source",
   "config": {
      "connector.class":"com.github.dariobalinzo.ElasticSourceConnector",
             "tasks.max": "1",
             "es.host" : "localhost",
             "es.port" : "9200",
             "index.prefix" : "my_awesome_index",
             "topic.prefix" : "es_",
             "incrementing.field.name" : "@timestamp"
        }
}
```
To start the connector with curl:
```bash
curl -X POST -H "Content-Type: application/json" --data @config.json http://localhost:8083/connectors | jq
  ```

To check the status:
```bash
curl localhost:8083/connectors/elastic-source/status | jq
  ```

To stop the connector:
```bash
curl -X DELETE localhost:8083/connectors/elastic-source | jq
```


## Documentation

### Elasticsearch Configuration

``es.host``
  ElasticSearch host. Optionally it is possible to specify many hosts using ``;`` as separator (``host1;host2;host3``) 

  * Type: string
  * Importance: high
  * Dependents: ``index.prefix``

``es.port``
  ElasticSearch port

  * Type: string
  * Importance: high
  * Dependents: ``index.prefix``
  
``es.scheme``
ElasticSearch scheme (http/https)

* Type: string
* Importance: medium
* Default: ``http``

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
  Indices prefix to include in copying.

  * Type: string
  * Default: ""
  * Importance: medium


### Connector Configuration

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
  
