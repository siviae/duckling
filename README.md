# Duckling

Duckling — пайплайн приземления данных из Kafka в Iceberg. Читает Avro-сообщения из Kafka-топиков, накапливает их в микро-батчи и записывает Parquet-файлы в DuckLake (каталог на Postgres + хранилище в S3). Отдельный экспортёр регистрирует эти файлы в REST-каталоге Iceberg (Lakekeeper) через чистую манипуляцию метаданными — без повторного чтения данных.

```
Kafka (Avro)
    ↓
Duckling (Kotlin)
    валидация → батч → flush
    ↓
DuckLake (каталог Postgres + Parquet в S3)
    ↓
Exporter (Python)
    fast_append только по метаданным
    ↓
Iceberg (Lakekeeper REST catalog + S3)
```

## Архитектура

**Одна корутина на Kafka-топик** под `SupervisorJob` — сбой в одном топике не затрагивает остальные.

**Три триггера flush** (срабатывает первый из наступивших):
- Прошедшее время с момента первой записи в батч (`min_flush_interval_seconds` до `max_flush_interval_seconds`)
- Прогресс по оффсетам: flush когда потреблена заданная доля от лага
- Накопленный объём в байтах: аварийный клапан, игнорирует минимальный интервал

**Адаптивный контроллер памяти** (`AdaptiveController.kt`): делит JVM-хип поровну между активными топиками и корректирует батч-бюджет и интервал flush через EMA-оценку пропускной способности. Предотвращает OOM при большом числе одновременно активных топиков.

**Управление схемами через Apicurio**: перед каждым flush загружается актуальная Avro-схема с BACKWARD-совместимостью. Изменения схемы вызывают `ALTER TABLE` DDL на таблице DuckLake. Несовместимые изменения типов останавливают топик.

**Экспортёр — только метаданные**: читает пути к файлам и количество записей напрямую из Postgres-каталога DuckLake (`ducklake_data_file`), затем вызывает PyIceberg `fast_append` для регистрации файлов в Lakekeeper. Parquet-файлы экспортёр не читает никогда.

## Структура модулей

```
duckling-landing/src/main/kotlin/io/company/duckling/
├── Main.kt                  # Загрузка конфига, Prometheus-сервер, корутин-супервизор
├── Pipeline.kt              # Машина состояний на топик: poll → validate → accumulate → flush → commit
├── AdaptiveController.kt    # EMA-оценка пропускной способности, батч-бюджет на топик
├── config/
│   ├── Config.kt            # Data-классы конфигурации
│   └── ConfigLoader.kt      # Парсер YAML на Jackson
├── source/
│   ├── KafkaSource.kt       # Обёртка KafkaConsumer: poll, watermarks, commitSync
│   └── SchemaValidator.kt   # Загрузка схем из Apicurio, маппинг Avro→DuckDB, правила валидации
└── sink/
    ├── DuckLakeWriter.kt    # DuckDB JDBC: ATTACH DuckLake, staged batch inserts
    └── SchemaEvolution.kt   # ALTER TABLE DDL: добавление колонок, расширение BIGINT→DOUBLE

duckling-exporter/
├── exporter.py              # Читает метаданные из Postgres DuckLake, регистрирует файлы в Iceberg
└── schema_sync.py           # Утилита: синхронизация схемы Iceberg из Apicurio Avro

integration_test/
└── test_landing_to_iceberg.py  # End-to-end pytest: Kafka → DuckLake → Iceberg
```

## Технологический стек

| Слой | Технология |
|------|-----------|
| Язык | Kotlin 2.2.0, JVM 21 |
| Сборка | Gradle (Kotlin DSL), fat JAR |
| Async | Kotlin Coroutines 1.8.0 |
| Kafka | kafka-clients 3.7.0 |
| Реестр схем | Apicurio 2.5.0 (BACKWARD compat, v2 API) |
| Сериализация | Avro 1.11.3 |
| Движок данных | DuckDB JDBC 1.5.1.0 (in-process, in-memory) |
| Хранилище | S3-совместимое объектное хранилище |
| Каталог | DuckLake 1.5.x (каталог Postgres + Parquet в S3) |
| Iceberg-каталог | Lakekeeper (`quay.io/lakekeeper/catalog:latest-main`) |
| Метрики | Micrometer Prometheus 1.12.0 |
| Конфигурация | Jackson YAML 2.17.0 |
| Экспортёр | Python 3.12, uv, duckdb 1.5.1+, pyiceberg 0.9+, psycopg2 |

## Локальный запуск

Поднять все сервисы:
```bash
docker compose up -d
```

Запустить интеграционный тест (1M записей end-to-end):
```bash
cd integration_test
uv run pytest test_landing_to_iceberg.py -s
```

Собрать JAR:
```bash
./gradlew :duckling-landing:jar
```

После изменений в коде — пересобрать и перезапустить:
```bash
./gradlew :duckling-landing:jar && docker compose up -d --build duckling
```

## Конфигурация

Файл конфигурации передаётся через `--config <path>` (по умолчанию: `duckling.yaml`).

```yaml
duckling:
  kafka:
    brokers: "kafka:9092"
    schema_registry: "http://apicurio:8080/apis/registry/v2"

  ducklake:
    catalog_url: "jdbc:postgresql://postgres:5432/ducklake?user=duckling&password=duckling"
    s3_endpoint: "http://<s3-host>:<port>"
    s3_access_key: "<access-key>"
    s3_secret_key: "<secret-key>"
    s3_region: "<region>"

  metrics:
    prometheus:
      port: 9090

  adaptive:
    safety_factor: 0.65   # доля максимального хипа, доступная Duckling
    ema_alpha: 0.3        # коэффициент сглаживания EMA пропускной способности

  topics:
    - name: team.orders
      group_id: duckling-team-orders
      table: team.orders        # isName=team, tableName=orders → таблица DuckLake team__orders
      min_flush_interval_seconds: 5
      max_flush_interval_seconds: 30
      metadata_poll_interval_seconds: 5
      offset_progress_flush_threshold: 0.5
      max_batch_bytes: 67108864  # верхняя граница 64 МБ (адаптивный контроллер может снижать)
```

Поле `table` имеет формат `{isName}.{tableName}`. Имя таблицы в DuckLake формируется как `{isName}__{tableName}` (например, `team.orders` → `team__orders`). Путь в S3: `s3://{isName}/ducklake/`.

## Метрики Prometheus (порт 9090)

| Метрика | Описание |
|---------|---------|
| `duckling_records_consumed_total` | Записей прочитано из Kafka (по топику) |
| `duckling_flushes_total` | Успешных записей в DuckLake (по топику) |
| `duckling_batch_flushed_bytes` | Гистограмма размеров батчей в байтах (по топику) |
| `duckling_topic_stopped_total` | Остановок топик-пайплайна с меткой причины |
| `duckling_adaptive_max_batch_bytes` | Текущий адаптивный байт-бюджет (по топику) |
| `duckling_adaptive_max_interval_seconds` | Текущий адаптивный интервал flush (по топику) |

## Обработка ошибок

- Ошибка валидации схемы → остановка корутины топика, инкремент `duckling_topic_stopped_total`
- Несовместимое изменение типа при эволюции схемы → остановка топика
- Ошибка записи → остановка топика (без молчаливой потери данных)
- Коммит оффсета происходит **только после** успешной записи — перезапуск без потерь

## Заметки о DuckLake

### Стратегия записи батча

Каждый flush сначала кладёт записи во временную таблицу DuckDB, затем делает единственную вставку:

```sql
CREATE OR REPLACE TEMP TABLE _batch_stage (col1 TYPE1, col2 TYPE2, ...);
-- JDBC prepared statement batch insert в _batch_stage
INSERT INTO ducklake.main.{table} SELECT * FROM _batch_stage;
DROP TABLE IF EXISTS _batch_stage;
```

Один `INSERT...SELECT` позволяет DuckLake управлять собственной транзакцией и создаёт ровно один Parquet-снепшот на flush. `DATA_INLINING_ROW_LIMIT 0` в ATTACH гарантирует, что все данные идут в S3 Parquet, а не инлайн в Postgres.

### Инспекция DuckLake

```bash
docker compose exec postgres psql -U duckling -d ducklake
```

```sql
SELECT table_name FROM ducklake_table;
SELECT COUNT(*) FROM ducklake_data_file;         -- записи о Parquet-файлах в S3
SELECT * FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 5;
SELECT changes_made FROM ducklake_snapshot_changes ORDER BY snapshot_id DESC LIMIT 10;
```
