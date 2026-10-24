# not-asap

Отложенные фоновые задачи поверх PostgreSQL для Go. Без брокера: только та база, что у сервиса уже есть.

*English: [README.md](README.md).*

- Типобезопасные обработчики на дженериках; payload хранится как JSON.
- Постановка внутри своей транзакции (`ScheduleTx`) для паттерна outbox.
- Отложенный запуск, ретраи с backoff, ключи идемпотентности, отмена.
- Свой пул воркеров на каждый kind, rescuer для зависших задач, janitor для завершённых.
- Graceful shutdown; метрики Prometheus в подпакете `prom` (опционально).

Требования: Go 1.26+, PostgreSQL 14+, `*pgxpool.Pool` из [pgx v5](https://github.com/jackc/pgx).

```bash
go get github.com/nottolstybai/not-asap
```

## Миграции

Схема — одна таблица `notasap_jobs`. Можно скопировать файл в свою директорию миграций или применить встроенный FS с **отдельной таблицей версий**, чтобы нумерация не пересекалась с миграциями сервиса.

- Файлы: `migrations/goose/00001_notasap_jobs.sql`, `migrations/golang-migrate/000001_notasap_jobs.{up,down}.sql`.
- Встроенные: `migrations.Goose` и `migrations.GolangMigrate` (пакет `github.com/nottolstybai/not-asap/migrations`).

goose:

```go
goose.SetBaseFS(migrations.Goose)
goose.SetTableName("notasap_goose_version")
err := goose.Up(db, "goose") // db — *sql.DB, например stdlib.OpenDBFromPool(pool)
```

или через provider API: `goose.NewProvider(goose.DialectPostgres, db, sub, goose.WithTableName("notasap_goose_version"))`, где `sub, _ := fs.Sub(migrations.Goose, "goose")`.

golang-migrate:

```go
src, _ := iofs.New(migrations.GolangMigrate, "golang-migrate")
m, _ := migrate.NewWithSourceInstance("iofs", src,
    dsn+"&x-migrations-table=notasap_schema_migrations") // в dsn уже есть query string
err := m.Up()
```

SQL не квалифицирует схему; при необходимости изолируйте таблицу через `search_path` соединения. Если таблицы нет, `Start` вернёт понятную ошибку.

## Быстрый старт

Полная программа — [examples/basic](examples/basic/main.go).

```go
const KindSendInvoice notasap.Kind = 1 // хранится в БД: не менять и не переиспользовать

type SendInvoice struct {
    OrderID int64  `json:"order_id"`
    Email   string `json:"email"`
}

func (SendInvoice) Kind() notasap.Kind { return KindSendInvoice }

client, err := notasap.New(pool)
err = notasap.Register(client, handle, notasap.KindConfig{Name: "send_invoice"})
err = client.Start(ctx) // не блокирует

id, err := client.Schedule(ctx, SendInvoice{OrderID: 42, Email: "a@b.c"},
    notasap.WithDelay(time.Minute),
    notasap.WithIdempotencyKey("invoice-42"))

err = client.Stop(shutdownCtx)
```

```go
func handle(ctx context.Context, job *notasap.Job[SendInvoice]) error {
    notasap.Logger(ctx).Info("sending", "order", job.Payload.OrderID) // в логгере job_id, kind, attempt
    return send(ctx, job.Payload)
}
```

Kind — число (`int16`, строго больше 0), хранится в БД. `KindConfig.Name` обязателен, уникален и используется только в логах и метриках. Тип payload — не указатель и не интерфейс. Регистрировать нужно до `Start`.

Жизненный цикл клиента одноразовый: `new -> started -> stopped`. Повторный `Start` вернёт `ErrAlreadyStarted`, `Start` после `Stop` — `ErrStopped`, `Stop` у незапущенного клиента вернёт `nil`. `Schedule*` и `Cancel` работают в любом состоянии: это просто запись в БД.

## Исходы обработчика

| Результат обработчика | Что происходит |
|---|---|
| `nil` | `done` |
| ошибка, оборачивающая `ErrCancel` | `cancelled`, текст ошибки сохраняется |
| ошибка, оборачивающая `ErrKeepAttempt` | снова `pending`, попытка **не тратится**, `run_at = NOW() + Backoff(job.Attempt)`. Лимита нет: используйте осторожно, следите за `reason=keep_attempt` |
| любая другая ошибка, паника, `Timeout` kind'а | `attempt >= MaxAttempts`: `failed`; иначе `pending`, `run_at = NOW() + Backoff(attempt)`. Ошибка сохраняется; паника пишется в лог со стеком |
| payload не удалось распаковать | сразу `failed` (ретрай бессмыслен) |
| прерван shutdown'ом (обработчик вернул ошибку) | `pending`, попытка возвращается, `run_at = NOW()` |
| вернул `nil`, несмотря на shutdown | `done` |

Текст ошибки обрезается до 4096 байт по границе руны.

Попытка тратится в момент claim, поэтому падение процесса тоже стоит попытки (защита от poison-задач). Доставка **at-least-once**: задача может выполниться не один раз (падение, rescue, shutdown), поэтому обработчики должны быть идемпотентными.

## Идемпотентность

`WithIdempotencyKey(key)` делает пару `(kind, key)` уникальной, пока строка существует. Строки удаляет janitor: ключ освобождается, когда удалена `done`/`cancelled` строка (`failed` по умолчанию хранится вечно).

| Существующая строка | без `WithReplace` | с `WithReplace` |
|---|---|---|
| нет | вставка | вставка |
| `pending`, `done`, `failed`, `cancelled` | `ErrDuplicate` + ID существующей | та же строка **перезаписывается** (ID сохраняется): новый payload, `pending`, `attempt=0`, `max_attempts` из текущего конфига, без ошибки, новый `run_at` |
| `running` | `ErrDuplicate` + ID | `ErrJobRunning` + ID |

`WithReplace` требует `WithIdempotencyKey`. В обоих случаях возвращается ID существующей задачи.

Особенности:

- `ScheduleTx` с `WithReplace` для выполняющейся строки вернёт `ErrJobRunning`, а перезаписанная строка остаётся заблокированной до конца вашей транзакции.
- Обычный дубль существующую строку не блокирует.
- `ScheduleBatch` (без tx) с ключами идемпотентности выполняется в собственной транзакции, т.к. разбор конфликтов может требовать нескольких round-trip'ов; без ключей — один round-trip.

## Постановка

```go
id, err := client.Schedule(ctx, p, opts...)
id, err := client.ScheduleTx(ctx, tx, p, opts...)          // видна после коммита tx
res, err := client.ScheduleBatch(ctx, notasap.Item(p1, opts...), notasap.Item(p2))
res, err := client.ScheduleBatchTx(ctx, tx, items...)
```

- `WithDelay(d)`: `run_at = NOW() + d` по часам БД.
- Kind без зарегистрированного обработчика даёт `ErrUnknownKind`, ничего не пишется.
- Батч атомарен. `ErrDuplicate`/`ErrJobRunning` по элементам приходят в `BatchResult{ID, Err}`; любая другая ошибка проваливает весь батч.
- `max_attempts` фиксируется в строке из конфига kind'а на момент постановки.
- Метрика `scheduled_total` увеличивается после успешного запроса, до коммита вашей транзакции, поэтому при rollback `*Tx`-вызова она завышена.

## Отмена

```go
err := client.Cancel(ctx, id)
```

`pending -> cancelled`. Для выполняющейся задачи вернёт `ErrJobRunning`, для уже завершённой (done/failed/cancelled) — `ErrJobFinished`, если строки нет — `ErrNotFound`. Чтобы отменить задачу изнутри обработчика, верните `ErrCancel`.

## Конфигурация

Нулевые поля берутся из `WithDefaults`, затем из встроенных значений.

| Опция | Поле | По умолчанию |
|---|---|---|
| `KindConfig` | `Workers` | 5 |
| | `MaxAttempts` | 3 |
| | `Timeout` | 1m |
| | `Backoff` | `Exponential(5s, 10m)` |
| | `PollInterval` | 1s |
| `WithRescuer(RescuerConfig)` | `Interval` | 30s |
| | `Grace` | 30s |
| `WithJanitor(JanitorConfig)` | `Interval` | 10m |
| | `BatchSize` | 1000 |
| | `Retention` | done 24h, cancelled 24h, failed 0 (не удалять) |
| | `Disabled` | false |
| `WithStopGrace(d)` | | 5s |
| `WithLogger(*slog.Logger)` | | `slog.Default()` |
| `WithObserver(Observer)` | | no-op |

- `WithDefaults(KindConfig{...})` задаёт общие умолчания для kind'ов (`Name` должен быть пустым).
- Backoff: `Constant(d)`, `Exponential(base, max)` (удвоение, jitter в `[d/2, d]`, без переполнения) или своя `func(attempt int) time.Duration`. Конструкторы паникуют на некорректных аргументах (как `regexp.MustCompile`), вызывайте их при инициализации.
- `Retention` janitor'а сливается с умолчаниями по ключам: `{StatusDone: time.Hour}` оставляет `cancelled` на 24h. Чтобы не удалять статус, укажите явный `0`. Допустимы только done, failed, cancelled.
- `Disabled` «липкий»: если какой-то `WithJanitor` его выставил, последующий `WithJanitor(JanitorConfig{})` janitor не включит обратно.
- Вся конфигурация проверяется в `New`/`Register` и возвращается ошибкой.

## Rescuer и долгие задачи

Взятая задача получает `deadline = claim + Timeout + Rescuer.Grace`. Rescuer (всегда включён) возвращает выполняющиеся задачи с истёкшим deadline в `pending` с backoff kind'а, либо в `failed`, если попытки кончились (`error = "rescued: deadline exceeded"`). Запоздалую запись старого воркера отсекает счётчик lease (fencing).

Heartbeat'а нет, поэтому задача упавшего процесса вернётся не раньше чем через `Timeout + Grace`. Для долгих задач ставьте большой `Timeout`.

Обработчик, игнорирующий `ctx`, продолжает занимать слот воркера (пул не запускает ту же задачу второй раз в этом процессе). Если он не вернулся через `Rescuer.Grace` после `Timeout`, пул пишет Error-лог и считает метрику `stuck`. Слот освободится, когда обработчик вернётся.

Задачи kind'а, не зарегистрированного в процессе, остаются `pending` и этим процессом не берутся; их заберёт процесс, где kind зарегистрирован.

## Остановка

`Stop(ctx)`:

1. прекращает опрос, останавливает rescuer и janitor;
2. ждёт выполняющиеся обработчики до `ctx.Done()`;
3. если `ctx` истёк, отменяет контексты обработчиков (причина: shutdown), ждёт `StopGrace`, пока они запишут статус, и возвращает `ctx.Err()`.

Прерванные задачи возвращаются в `pending` с возвратом попытки. Если `Stop` истёк по таймауту, обработчики, игнорирующие контекст, могут продолжать работу после его возврата; их задачи подберёт rescuer.

## Метрики

Ядро без зависимостей, оно сообщает события в `Observer`. Подпакет `prom` реализует его для Prometheus:

```go
m, err := prom.New(prometheus.DefaultRegisterer,
    prom.WithJobDurationBuckets([]float64{0.1, 1, 10, 60}),
    prom.WithPollDurationBuckets([]float64{0.001, 0.01, 0.1}),
)
client, err := notasap.New(pool, notasap.WithObserver(m))
```

`prom.New` возвращает ошибку (не паникует) при nil-registerer'е, повторной регистрации или некорректных бакетах. Бакеты по умолчанию: длительность задачи `ExponentialBuckets(0.005, 2, 15)`, опрос `ExponentialBuckets(0.0005, 2, 12)`. Метка `kind` — это `KindConfig.Name`.

| Метрика | Метки |
|---|---|
| `notasap_jobs_scheduled_total` | kind |
| `notasap_jobs_finished_total` | kind, status (done/failed/cancelled) |
| `notasap_jobs_retried_total` | kind, reason (error/panic/timeout/keep_attempt/shutdown) |
| `notasap_job_duration_seconds` | kind, outcome (done/failed/cancelled/retried) |
| `notasap_jobs_running` | kind |
| `notasap_poll_duration_seconds` | kind |
| `notasap_jobs_rescued_total` | kind |
| `notasap_jobs_stuck_total` | kind |
| `notasap_janitor_deleted_total` | status |

Для другой системы метрик реализуйте `notasap.Observer`; встройте `notasap.NopObserver`, чтобы переопределить лишь нужные методы. Реализация должна быть потокобезопасной и не блокироваться.

## Разработка

```bash
make test    # go test -race ./... (интеграционным тестам нужен Docker)
make cover   # покрытие по пакетам
make vet
```

Интеграционные тесты используют testcontainers (`postgres:16-alpine`) и пропускаются при `-short`.
