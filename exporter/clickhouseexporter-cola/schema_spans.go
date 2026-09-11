// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import "fmt"

const spansMVName = "otel_span_mv"

func spanSchemaStatements(onCluster, dbName, tracesName string) []string {
	spans := qualified(dbName, defaultSpansTable(""))
	mv := qualified(dbName, spansMVName)
	traces := qualified(dbName, tracesName)
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s%s
(
	Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
	TraceId String CODEC(ZSTD(1)),
	SpanId String CODEC(ZSTD(1)),
	ParentSpanId String CODEC(ZSTD(1)),
	ServiceName LowCardinality(String) CODEC(ZSTD(1)),
	SpanKind LowCardinality(String) CODEC(ZSTD(1)),
	SpanName LowCardinality(String) CODEC(ZSTD(1)),
	ScopeName LowCardinality(String) CODEC(ZSTD(1)),
	Duration UInt64 CODEC(ZSTD(1)),
	StatusCode LowCardinality(String) CODEC(ZSTD(1)),
	db_system LowCardinality(String) CODEC(ZSTD(1)),
	db_type LowCardinality(String) CODEC(ZSTD(1)),
	messaging_system LowCardinality(String) CODEC(ZSTD(1)),
	rpc_system LowCardinality(String) CODEC(ZSTD(1)),
	peer_host String CODEC(ZSTD(1)),
	peer_port LowCardinality(String) CODEC(ZSTD(1)),
	svc_host String CODEC(ZSTD(1)),
	svc_port LowCardinality(String) CODEC(ZSTD(1)),
	span_url String CODEC(ZSTD(1)),
	mw_name LowCardinality(String) CODEC(ZSTD(1)),
	INDEX idx_timestamp Timestamp TYPE minmax GRANULARITY 3
)
ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
PRIMARY KEY (SpanKind, Timestamp)
ORDER BY (SpanKind, Timestamp, TraceId, SpanId)
TTL toDateTime(Timestamp) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`, spans, onCluster),
		fmt.Sprintf(`CREATE MATERIALIZED VIEW IF NOT EXISTS %s%s TO %s AS
SELECT
	Timestamp, TraceId, SpanId, ParentSpanId,
	ServiceName, SpanKind, SpanName, ScopeName,
	Duration, StatusCode,
	db_system, db_type, messaging_system, rpc_system,
	peer_host, peer_port, svc_host, svc_port, span_url,
	multiIf(
		(db_lc = 'redis' OR type_lc = 'redis' OR msg_lc = 'redis' OR rpc_lc = 'redis' OR rpc_lc = 'apache_redis'), 'Redis',
		(db_lc = 'memcached' OR type_lc = 'memcached' OR msg_lc = 'memcached' OR rpc_lc = 'memcached' OR rpc_lc = 'apache_memcached'), 'Memcached',
		(db_lc = 'kafka' OR type_lc = 'kafka' OR msg_lc = 'kafka' OR rpc_lc = 'kafka' OR rpc_lc = 'apache_kafka'), 'Kafka',
		(db_lc = 'rabbitmq' OR type_lc = 'rabbitmq' OR msg_lc = 'rabbitmq' OR rpc_lc = 'rabbitmq' OR rpc_lc = 'apache_rabbitmq'), 'RabbitMQ',
		(db_lc = 'rocketmq' OR type_lc = 'rocketmq' OR msg_lc = 'rocketmq' OR rpc_lc = 'rocketmq' OR rpc_lc = 'apache_rocketmq'), 'RocketMQ',
		(db_lc = 'mariadb' OR type_lc = 'mariadb' OR msg_lc = 'mariadb' OR rpc_lc = 'mariadb' OR rpc_lc = 'apache_mariadb'), 'MariaDB',
		(db_lc = 'tidb' OR type_lc = 'tidb' OR msg_lc = 'tidb' OR rpc_lc = 'tidb' OR rpc_lc = 'apache_tidb'), 'TiDB',
		(db_lc = 'oceanbase' OR type_lc = 'oceanbase' OR msg_lc = 'oceanbase' OR rpc_lc = 'oceanbase' OR rpc_lc = 'apache_oceanbase'), 'OceanBase',
		(db_lc = 'clickhouse' OR type_lc = 'clickhouse' OR msg_lc = 'clickhouse' OR rpc_lc = 'clickhouse' OR rpc_lc = 'apache_clickhouse'), 'ClickHouse',
		(db_lc = 'postgresql' OR type_lc = 'postgresql' OR msg_lc = 'postgresql' OR rpc_lc = 'postgresql' OR rpc_lc = 'apache_postgresql'), 'PostgreSQL',
		(db_lc = 'sqlserver' OR type_lc = 'sqlserver' OR msg_lc = 'sqlserver' OR rpc_lc = 'sqlserver' OR rpc_lc = 'apache_sqlserver'), 'SQL Server',
		(db_lc = 'oracle' OR type_lc = 'oracle' OR msg_lc = 'oracle' OR rpc_lc = 'oracle' OR rpc_lc = 'apache_oracle'), 'Oracle',
		(db_lc = 'dameng' OR type_lc = 'dameng' OR msg_lc = 'dameng' OR rpc_lc = 'dameng' OR rpc_lc = 'apache_dameng'), '达梦',
		(db_lc = 'gaussdb' OR type_lc = 'gaussdb' OR msg_lc = 'gaussdb' OR rpc_lc = 'gaussdb' OR rpc_lc = 'apache_gaussdb'), 'GaussDB',
		(db_lc = 'kingbase' OR type_lc = 'kingbase' OR msg_lc = 'kingbase' OR rpc_lc = 'kingbase' OR rpc_lc = 'apache_kingbase'), '人大金仓',
		(db_lc = 'mysql' OR type_lc = 'mysql' OR msg_lc = 'mysql' OR rpc_lc = 'mysql' OR rpc_lc = 'apache_mysql'), 'MySQL',
		(db_lc = 'jdbc' OR type_lc = 'jdbc' OR msg_lc = 'jdbc' OR rpc_lc = 'jdbc' OR rpc_lc = 'apache_jdbc'), 'JDBC',
		(db_lc = 'mongodb' OR type_lc = 'mongodb' OR msg_lc = 'mongodb' OR rpc_lc = 'mongodb' OR rpc_lc = 'apache_mongodb'), 'MongoDB',
		(db_lc = 'elasticsearch' OR type_lc = 'elasticsearch' OR msg_lc = 'elasticsearch' OR rpc_lc = 'elasticsearch' OR rpc_lc = 'apache_elasticsearch'), 'Elasticsearch',
		(db_lc = 'nacos' OR type_lc = 'nacos' OR msg_lc = 'nacos' OR rpc_lc = 'nacos' OR rpc_lc = 'apache_nacos'), 'Nacos',
		(db_lc = 'minio' OR type_lc = 'minio' OR msg_lc = 'minio' OR rpc_lc = 'minio' OR rpc_lc = 'apache_minio'), 'MinIO',
		(db_lc = 'oss' OR type_lc = 'oss' OR msg_lc = 'oss' OR rpc_lc = 'oss' OR rpc_lc = 'apache_oss'), 'Aliyun OSS',
		(db_lc = 'dubbo' OR type_lc = 'dubbo' OR msg_lc = 'dubbo' OR rpc_lc = 'dubbo' OR rpc_lc = 'apache_dubbo'), 'Dubbo',
		(db_lc = 'grpc' OR type_lc = 'grpc' OR msg_lc = 'grpc' OR rpc_lc = 'grpc' OR rpc_lc = 'apache_grpc'), 'gRPC',
		(db_lc = 'feign' OR type_lc = 'feign' OR msg_lc = 'feign' OR rpc_lc = 'feign' OR rpc_lc = 'apache_feign'), 'Feign',
		(db_lc = 'okhttp' OR type_lc = 'okhttp' OR msg_lc = 'okhttp' OR rpc_lc = 'okhttp' OR rpc_lc = 'apache_okhttp'), 'OkHttp',
		(db_lc = 'httpclient' OR type_lc = 'httpclient' OR msg_lc = 'httpclient' OR rpc_lc = 'httpclient' OR rpc_lc = 'apache_httpclient'), 'HttpClient',
		(db_lc = 'xxljob' OR type_lc = 'xxljob' OR msg_lc = 'xxljob' OR rpc_lc = 'xxljob' OR rpc_lc = 'apache_xxljob'), 'XXL-JOB',
		position(name_lc, 'redisson') > 0 OR position(name_lc, 'lettuce') > 0 OR position(name_lc, 'jedis') > 0 OR position(name_lc, 'redis') > 0, 'Redis',
		position(name_lc, 'memcached') > 0 OR position(name_lc, 'memcache') > 0, 'Memcached',
		position(name_lc, 'kafka') > 0, 'Kafka',
		position(name_lc, 'rabbitmq') > 0 OR position(name_lc, 'spring-rabbit') > 0 OR position(name_lc, 'amqp') > 0, 'RabbitMQ',
		position(name_lc, 'rocketmq') > 0, 'RocketMQ',
		position(name_lc, 'mariadb') > 0, 'MariaDB',
		position(name_lc, 'tidb') > 0, 'TiDB',
		position(name_lc, 'oceanbase') > 0, 'OceanBase',
		(position(name_lc, 'clickhouse') > 0 OR position(name_lc, 'clickhouse-jdbc') > 0) OR (db_lc = 'clickhouse' OR peer_port IN ('8123', '8443') OR svc_port IN ('8123', '8443') OR position(span_url, ':8123') > 0 OR position(span_url, ':8443') > 0 OR position(peer_lc, 'clickhouse') > 0), 'ClickHouse',
		position(name_lc, 'postgresql') > 0 OR position(name_lc, 'postgres') > 0, 'PostgreSQL',
		position(name_lc, 'sqlserver') > 0 OR position(name_lc, 'sql server') > 0 OR position(name_lc, 'mssql') > 0 OR position(name_lc, 'microsoft.sql') > 0, 'SQL Server',
		position(name_lc, 'oracle') > 0, 'Oracle',
		position(name_lc, 'dameng') > 0 OR position(name_lc, 'dm.jdbc') > 0 OR position(name_lc, 'dm8') > 0, '达梦',
		position(name_lc, 'gaussdb') > 0 OR position(name_lc, 'opengauss') > 0, 'GaussDB',
		position(name_lc, 'kingbase') > 0, '人大金仓',
		position(name_lc, 'mysql') > 0, 'MySQL',
		position(name_lc, 'hikaricp') > 0 OR position(name_lc, 'hikari') > 0 OR position(name_lc, 'alibaba.druid') > 0 OR position(name_lc, 'hibernate') > 0 OR position(name_lc, 'mybatis') > 0 OR position(name_lc, 'jdbctemplate') > 0 OR position(name_lc, 'r2dbc') > 0 OR position(name_lc, 'jdbc') > 0 OR position(name_lc, 'druid') > 0, 'JDBC',
		position(name_lc, 'mongodb') > 0 OR position(name_lc, 'mongo') > 0, 'MongoDB',
		position(name_lc, 'elasticsearch') > 0 OR position(name_lc, 'co.elastic') > 0, 'Elasticsearch',
		position(name_lc, 'nacos') > 0, 'Nacos',
		position(name_lc, 'minio') > 0, 'MinIO',
		position(name_lc, 'aliyun-oss') > 0 OR position(name_lc, 'oss.aliyun') > 0 OR position(name_lc, 'aliyun.oss') > 0, 'Aliyun OSS',
		position(name_lc, 'dubbo') > 0, 'Dubbo',
		position(name_lc, 'grpc') > 0, 'gRPC',
		position(name_lc, 'feign') > 0 OR position(name_lc, 'openfeign') > 0, 'Feign',
		position(name_lc, 'okhttp') > 0, 'OkHttp',
		position(name_lc, 'apache-httpclient') > 0 OR position(name_lc, 'http-url-connection') > 0 OR position(name_lc, 'httpclient') > 0, 'HttpClient',
		position(name_lc, 'xxl-job') > 0 OR position(name_lc, 'xxljob') > 0, 'XXL-JOB',
		''
	) AS mw_name
FROM (
	SELECT
		Timestamp, TraceId, SpanId, ParentSpanId,
		ServiceName, SpanKind, SpanName, ScopeName,
		Duration, StatusCode,
		db_system, db_type, messaging_system, rpc_system,
		peer_host, peer_port, svc_host, svc_port, span_url,
		lowerUTF8(db_system) AS db_lc,
		lowerUTF8(db_type) AS type_lc,
		lowerUTF8(messaging_system) AS msg_lc,
		lowerUTF8(rpc_system) AS rpc_lc,
		lowerUTF8(concat(ScopeName, ' ', SpanName)) AS name_lc,
		lowerUTF8(concat(peer_host, ' ', span_url)) AS peer_lc
	FROM (
		SELECT
			Timestamp, TraceId, SpanId, ParentSpanId,
			ServiceName, SpanKind, SpanName, ScopeName,
			Duration, StatusCode,
			ifNull(SpanAttributes['db.system'], '') AS db_system,
			ifNull(SpanAttributes['db.type'], '') AS db_type,
			ifNull(SpanAttributes['messaging.system'], '') AS messaging_system,
			ifNull(SpanAttributes['rpc.system'], '') AS rpc_system,
			coalesce(
				nullIf(SpanAttributes['network.peer.address'], ''),
				nullIf(SpanAttributes['net.peer.ip'], ''),
				nullIf(SpanAttributes['net.sock.peer.addr'], ''),
				nullIf(SpanAttributes['server.address'], ''),
				nullIf(SpanAttributes['server.socket.address'], ''),
				nullIf(SpanAttributes['net.peer.name'], ''),
				nullIf(SpanAttributes['peer.service'], ''),
				''
			) AS peer_host,
			coalesce(
				nullIf(SpanAttributes['network.peer.port'], ''),
				nullIf(SpanAttributes['net.peer.port'], ''),
				nullIf(SpanAttributes['server.port'], ''),
				nullIf(SpanAttributes['net.sock.peer.port'], ''),
				''
			) AS peer_port,
			coalesce(
				nullIf(ResourceAttributes['host.ip'], ''),
				nullIf(ResourceAttributes['net.host.ip'], ''),
				nullIf(SpanAttributes['net.host.ip'], ''),
				nullIf(ResourceAttributes['net.sock.host.addr'], ''),
				nullIf(ResourceAttributes['net.host.name'], ''),
				nullIf(ResourceAttributes['host.name'], ''),
				nullIf(ResourceAttributes['service.instance.id'], ''),
				''
			) AS svc_host,
			coalesce(
				nullIf(ResourceAttributes['net.host.port'], ''),
				nullIf(SpanAttributes['net.host.port'], ''),
				nullIf(SpanAttributes['server.port'], ''),
				''
			) AS svc_port,
			coalesce(
				nullIf(SpanAttributes['url.full'], ''),
				nullIf(SpanAttributes['http.url'], ''),
				nullIf(SpanAttributes['url'], ''),
				''
			) AS span_url
		FROM %s
	)
)`, mv, onCluster, spans, traces),
	}
}
