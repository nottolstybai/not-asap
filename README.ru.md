# not-asap

[![Go Reference](https://pkg.go.dev/badge/github.com/nottolstybai/not-asap.svg)](https://pkg.go.dev/github.com/nottolstybai/not-asap)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue)](https://go.dev/)

**not-asap** — Go-библиотека для фоновой обработки задач поверх PostgreSQL. Без Redis, без RabbitMQ, без отдельного брокера — только та база, что уже есть у вашего сервиса.

*Read in English — [README.md](README.md).*

## Возможности

- Постановка задач в очередь внутри существующей бизнес-транзакции (`EnqueueTx`) — outbox-паттерн из коробки.
- Типобезопасные хендлеры на дженериках — payload приходит в handler уже распарсенным, а не сырыми байтами.
- Ретраи с настраиваемым backoff (static или exponential) глобально или на тип задачи.
- Отмена задачи без дальнейших попыток через `ErrCancel`.
- Batch-вставка нескольких задач одним round-trip.
- Graceful shutdown: прерванные задачи не теряются.
- Автоматическая очистка завершённых строк (janitor) и Prometheus-метрики — опционально.

## Требования

- Go 1.21+
- PostgreSQL 12+
- `*pgxpool.Pool` из [`pgx/v5`](https://github.com/jackc/pgx) — библиотека принимает уже готовый пул.

## Установка

```bash
go get github.com/nottolstybai/not-asap
```

## Применение миграций

Перед запуском очереди накатите миграцию ниже любым удобным инструментом.

<details>
<summary>goose</summary>

```sql
-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS notasap_jobs (
    id                UUID        NOT NULL DEFAULT gen_random_uuid(),
    job_type          TEXT        NOT NULL,
    payload           JSONB       NOT NULL DEFAULT '{}',
    status            TEXT        NOT NULL DEFAULT 'new',
    run_after         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt           INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 3,
    error             TEXT,
    last_heartbeat_at TIMESTAMPTZ,
    worker_id         TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT notasap_jobs_pkey PRIMARY KEY (id),
    CONSTRAINT notasap_jobs_status_check CHECK (
        status IN ('new','running','done','failed','retrying','exhausted','cancelled')
    )
);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_notasap_jobs_poll
    ON notasap_jobs (status, run_after)
    WHERE status IN ('new', 'retrying');
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_notasap_jobs_heartbeat
    ON notasap_jobs (status, last_heartbeat_at)
    WHERE status = 'running';
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_notasap_jobs_janitor
    ON notasap_jobs (status, updated_at)
    WHERE status IN ('done', 'failed', 'exhausted', 'cancelled');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS notasap_jobs;
-- +goose StatementEnd
```

</details>

<details>
<summary>golang-migrate</summary>

`000001_notasap_jobs.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS notasap_jobs (
    id                UUID        NOT NULL DEFAULT gen_random_uuid(),
    job_type          TEXT        NOT NULL,
    payload           JSONB       NOT NULL DEFAULT '{}',
    status            TEXT        NOT NULL DEFAULT 'new',
    run_after         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt           INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 3,
    error             TEXT,
    last_heartbeat_at TIMESTAMPTZ,
    worker_id         TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT notasap_jobs_pkey PRIMARY KEY (id),
    CONSTRAINT notasap_jobs_status_check CHECK (
        status IN ('new','running','done','failed','retrying','exhausted','cancelled')
    )
);

CREATE INDEX IF NOT EXISTS idx_notasap_jobs_poll
    ON notasap_jobs (status, run_after)
    WHERE status IN ('new', 'retrying');

CREATE INDEX IF NOT EXISTS idx_notasap_jobs_heartbeat
    ON notasap_jobs (status, last_heartbeat_at)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_notasap_jobs_janitor
    ON notasap_jobs (status, updated_at)
    WHERE status IN ('done', 'failed', 'exhausted', 'cancelled');
```

`000001_notasap_jobs.down.sql`:

```sql
DROP TABLE IF EXISTS notasap_jobs;
```

</details>

<details>
<summary>Liquibase</summary>

```xml
<changeSet id="notasap-jobs-1" author="you">
    <sql splitStatements="true" endDelimiter=";">
        CREATE TABLE IF NOT EXISTS notasap_jobs (
            id                UUID        NOT NULL DEFAULT gen_random_uuid(),
            job_type          TEXT        NOT NULL,
            payload           JSONB       NOT NULL DEFAULT '{}',
            status            TEXT        NOT NULL DEFAULT 'new',
            run_after         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            attempt           INT         NOT NULL DEFAULT 0,
            max_attempts      INT         NOT NULL DEFAULT 3,
            error             TEXT,
            last_heartbeat_at TIMESTAMPTZ,
            worker_id         TEXT,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT notasap_jobs_pkey PRIMARY KEY (id),
            CONSTRAINT notasap_jobs_status_check CHECK (
                status IN ('new','running','done','failed','retrying','exhausted','cancelled')
            )
        );
        CREATE INDEX IF NOT EXISTS idx_notasap_jobs_poll
            ON notasap_jobs (status, run_after)
            WHERE status IN ('new', 'retrying');
        CREATE INDEX IF NOT EXISTS idx_notasap_jobs_heartbeat
            ON notasap_jobs (status, last_heartbeat_at)
            WHERE status = 'running';
        CREATE INDEX IF NOT EXISTS idx_notasap_jobs_janitor
            ON notasap_jobs (status, updated_at)
            WHERE status IN ('done', 'failed', 'exhausted', 'cancelled');
    </sql>
    <rollback>
        DROP TABLE IF EXISTS notasap_jobs;
    </rollback>
</changeSet>
```

</details>

## Быстрый старт

### 1. Опишите payload задачи

Определите структуру с данными задачи и реализуйте интерфейс `JobArgs`:

```go
type JobArgs interface {
    Kind() string // уникальный идентификатор, хранится в колонке job_type
}
```

```go
type SendEmail struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

func (SendEmail) Kind() string { return "send_email" }
```

Подходит любая JSON-сериализуемая структура. `Kind()` — строка, по которой очередь сопоставляет строку из БД с нужным хендлером.

### 2. Создайте очередь

```go
cfg, err := notasap.NewQueueConfig(func(c *notasap.QueueConfig) {
    c.WorkerCount   = 10
    c.MaxRetries    = 5
    c.RetryStrategy = notasap.RetryStrategyExponential
})
if err != nil {
    log.Fatal(err)
}

q := notasap.NewQueue(pool, cfg)
```

Все доступные параметры и их значения по умолчанию — в разделе [Конфигурация](#конфигурация).

### 3. Зарегистрируйте хендлер

Хендлеры имеют сигнатуру `func(ctx context.Context, args T) error`, где `T` реализует `JobArgs`. Очередь сама десериализует payload в `T` перед вызовом хендлера — никаких сырых байт.

```go
notasap.Register(q, func(ctx context.Context, job SendEmail) error {
    return mailer.Send(job.To, job.Subject)
})
```

Третий аргумент `Register` — опциональный override конфигурации для конкретного типа задачи. Если не передан, тип наследует настройки из `QueueConfig`:

```go
notasap.Register(q, sendEmailHandler, func(tc *notasap.TaskConfig) {
    tc.MaxRetries = 10
    tc.RetryDelay = 30 * time.Second
})
```

### 4. Запустите и остановите пул воркеров

```go
q.Start()
defer q.Stop(context.Background())
```

### 5. Поставьте задачу в очередь

```go
job, err := q.Enqueue(ctx, SendEmail{To: "a@b.co", Subject: "hi"})
```

`Enqueue` возвращает `*Job` с полями `ID`, `JobType`, `Status`, `MaxAttempts` и `CreatedAt`. Если идентификатор задачи не нужен, результат можно проигнорировать.

## Постановка в транзакции (outbox)

Главная причина выбирать очередь на PostgreSQL — ставить задачу в той же транзакции, что и изменение бизнес-данных. Либо оба изменения закоммитятся, либо ни одного:

```go
tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

if _, err := tx.Exec(ctx, "INSERT INTO orders (...) VALUES (...)"); err != nil {
    return err
}

if _, err := q.EnqueueTx(ctx, tx, SendEmail{To: "a@b.co"}); err != nil {
    return err
}

return tx.Commit(ctx)
```

Задача станет видна воркерам только после `tx.Commit`.

## Batch-вставка

```go
err := q.EnqueueBatch(ctx,
    SendEmail{To: "a@b.co", Subject: "hi"},
    SendEmail{To: "c@d.co", Subject: "hey"},
)
```

Также доступен `EnqueueBatchTx` для транзакционной batch-вставки.

## Отмена задачи без ретрая

Верните `notasap.ErrCancel` из хендлера — задача сразу перейдёт в терминальный статус `cancelled`, оставшиеся попытки не используются:

```go
notasap.Register(q, func(ctx context.Context, job SendEmail) error {
    if !userExists(job.To) {
        return notasap.ErrCancel
    }
    return mailer.Send(job.To, job.Subject)
})
```

Любая другая ошибка из хендлера приведёт к обычному ретраю согласно настройкам.

## Конфигурация

### Глобально — `QueueConfig`

Передаётся в `NewQueueConfig` и применяется ко всей очереди.

| Поле | Default | Назначение |
|------|---------|------------|
| `WorkerCount` | `5` | Количество параллельных воркеров на инстанс |
| `PollInterval` | `2s` | Пауза между опросами БД, если очередь пуста |
| `MaxRetries` | `3` | Максимум попыток до перехода в `exhausted` |
| `RetryDelay` | `5s` | База задержки между попытками |
| `RetryStrategy` | `Exponential` | `RetryStrategyStatic` или `RetryStrategyExponential` |
| `MaxRetryDelay` | `10m` | Потолок задержки для exponential |
| `HeartbeatInterval` | `15s` | Частота обновления heartbeat выполняющейся задачи |
| `ShutdownTimeout` | `30s` | Сколько ждать завершения задач при `Stop` |
| `Logger` | `slog.Default()` | Инъекция `*slog.Logger` |
| `Registerer` | `nil` | `prometheus.Registerer`; `nil` выключает метрики |
| `Janitor` | см. ниже | Автоудаление терминальных задач |

### На тип задачи — `TaskConfig`

Передаётся опциональным третьим аргументом `Register`. Пустые поля наследуют значение из `QueueConfig`. Пример использования — в [шаге 3](#3-зарегистрируйте-хендлер).

| Поле | Назначение |
|------|------------|
| `MaxRetries` | Лимит попыток для этого типа задачи |
| `RetryDelay` | База задержки |
| `RetryStrategy` | `RetryStrategyStatic` или `RetryStrategyExponential` |
| `MaxRetryDelay` | Потолок задержки для exponential |

### Janitor

```go
cfg.Janitor = notasap.JanitorConfig{
    TTL:         24 * time.Hour,
    Statuses:    []notasap.Status{notasap.StatusDone, notasap.StatusCancelled},
    RunInterval: 1 * time.Hour,
    Disabled:    false,
}
```

| Поле | Назначение |
|------|------------|
| `TTL` | Сколько терминальная задача живёт до удаления |
| `Statuses` | Какие статусы чистить (по умолчанию `done`, `exhausted`, `cancelled`) |
| `RunInterval` | Как часто запускать чистку |
| `Disabled` | `true` — полностью выключить janitor |

## Статусы задачи

Публичные константы — используйте их для фильтрации и проверок:

| Статус | Когда присваивается |
|--------|---------------------|
| `StatusNew` | Задача только что добавлена |
| `StatusRunning` | Воркер взял задачу в работу |
| `StatusRetrying` | Попытка неуспешна, ждёт следующий attempt |
| `StatusDone` | Хендлер вернул `nil` |
| `StatusCancelled` | Хендлер вернул `ErrCancel` |
| `StatusExhausted` | Исчерпаны все попытки |
| `StatusFailed` | Зафиксирован неустранимый сбой (паника, отсутствие хендлера) |

## Логирование

Передайте `*slog.Logger` в `QueueConfig.Logger`. Внутри хендлера вызовите `notasap.LoggerFromContext(ctx)` — получите логгер с уже добавленными полями задачи:

| Ключ | Тип | Значение |
|------|-----|----------|
| `job_id` | `string` | UUID текущей задачи |
| `job_type` | `string` | Значение, возвращаемое `Kind()` |
| `attempt` | `int` | Номер текущей попытки (с 1) |

```go
notasap.Register(q, func(ctx context.Context, job SendEmail) error {
    log := notasap.LoggerFromContext(ctx)
    log.Info("sending", "to", job.To)
    return mailer.Send(job.To, job.Subject)
})
```

## Prometheus-метрики

Передайте свой `prometheus.Registerer` в `QueueConfig.Registerer` — метрики подпишутся автоматически. Если поле оставить `nil`, метрики не собираются.

```go
reg := prometheus.NewRegistry()
cfg.Registerer = reg
```

Экспортируемые метрики:

| Метрика | Тип | Лейблы |
|---------|-----|--------|
| `notasap_jobs_enqueued_total` | Counter | `job_type` |
| `notasap_jobs_processed_total` | Counter | `job_type`, `status` |
| `notasap_job_duration_seconds` | Histogram | `job_type` |
| `notasap_jobs_in_flight` | Gauge | — |
| `notasap_poll_duration_seconds` | Histogram | — |

## Graceful shutdown

```go
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
<-sigCh

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := q.Stop(ctx); err != nil {
    log.Printf("shutdown: %v", err)
}
```

`Stop` перестаёт забирать новые задачи, ждёт завершения уже запущенных до `ShutdownTimeout`, а затем отменяет их контексты. Прерванные задачи возвращаются в `retrying` и будут подхвачены на следующем опросе.

## Playground

В каталоге [`examples/playground/`](examples/playground/README.md) лежит самодостаточное демо с HTMX-интерфейсом, Prometheus и Grafana. Одна команда `docker compose up` (или `make up` из этого каталога) поднимает весь стек с предпровиженным дашбордом.

## License

MIT
