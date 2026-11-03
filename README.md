# OMS5 Operations Service

`OMS5` - микросервис операционного контура. Он владеет бизнес-сущностями `Клиент`, `Смена`, `Исполнитель`, `Табель` и статусной моделью смен.

## Почему эти сущности объединены

На старте эти сущности тесно связаны одним бизнес-процессом:

- клиент заказывает работы;
- под клиента создаются смены;
- смены предлагаются нескольким исполнителям;
- первый откликнувшийся исполнитель назначается на смену;
- по сменам и исполнителям формируется табель.

Поэтому отдельные микросервисы для каждой сущности пока были бы избыточны. Когда модель усложнится, можно выделить `Client Service`, `Planning Service` и `Timesheet Service`.

## Функции

- Клиенты, смены и FSM смен.
- Проекция профиля исполнителя из OMS2: тип оформления, подтверждение, стадия и счётчик смен.
- Предложения только исполнителям с подтверждённым типом и готовой стадией.
- Транзакционный outbox публикует факты назначения и завершения в Kafka с повторной отправкой.
- Табели и сводка по операционным данным.
- Проверка пользовательских токенов через OMS1 и ролевой доступ к операциям операторов.

## Технологии

- Python 3.11
- FastAPI
- Uvicorn
- PostgreSQL для постоянного операционного состояния
- SQL-миграции и транзакции PostgreSQL
- confluent-kafka

## Термины

- Shift / смена - центральная бизнес-сущность OMS5.
- Performer / исполнитель - локальная проекция профиля исполнителя из OMS2.
- User / пользователь портала - аккаунт и роли в OMS1.

## Границы владения

- OMS5 владеет клиентами, сменами, проекцией исполнителя, предложениями, назначениями, табелями и FSM смен.
- OMS2 владеет профилем исполнителя и персональными данными; OMS5 хранит операционную проекцию.
- OMS1 владеет учетными записями, ролями, JWT и связью user -> person_id.
- OMS4 владеет шаблонами уведомлений и доставкой email.

## Доступ

Защищенные операции оператора проверяют Bearer-токен через OMS1 и требуют роль manager, supervisor или administrator. Внешний пользователь создает смены только в рамках своей организации. Внутренняя синхронизация /performers/sync требует X-Service-Token от OMS2.

## Статусы смены

```text
A00_DRAFT       Создание
A10_CONFIRM     Подтверждение
A20_SOURCING    Подбор исполнителя
A30_CHOICE      Ожидание начала смены
A40_EXECUTION   Исполнение
A50_VERIFY      Проверка / подтверждение табеля
A60_SETTLE      Оплата
A70_CONFIRM     Закрывающие документы
A80_CLOSED      Закрыта
A90_ARCHIVE     Архив
```

История переходов хранится в `ShiftStatusHistory`.

Разрешенные переходы:

```text
A00_DRAFT -> A10_CONFIRM
A10_CONFIRM -> A20_SOURCING
A20_SOURCING -> A30_CHOICE
A20_SOURCING -> A80_CLOSED
A30_CHOICE -> A20_SOURCING
A30_CHOICE -> A40_EXECUTION
A30_CHOICE -> A80_CLOSED
A40_EXECUTION -> A50_VERIFY
A50_VERIFY -> A60_SETTLE
A50_VERIFY -> A80_CLOSED
A60_SETTLE -> A70_CONFIRM
A70_CONFIRM -> A80_CLOSED
A80_CLOSED -> A90_ARCHIVE
```

Для `A80_CLOSED` используются `close_reason`: `paid`, `canceled`, `failed`, `deleted`.

Для неуспешного закрытия используются `failure_reason`: `absence`, `no_performer_found`, `client_rejected`, `timesheet_invalid`, `manual_admin_decision`.

## Reporting period

Отчетный период считается по `shift.starts_at`. Дата закрытия отчетного периода - конец 10-го числа месяца, следующего за месяцем `shift.starts_at`.

Если отчетный период уже закрыт, создание смены задним числом отклоняется с `409 reporting_period_closed`.

Для незаполненных смен в статусе `A20_SOURCING` есть internal endpoint авто-закрытия:

```http
POST /internal/shifts/auto-close-unfilled
```

Он переводит подходящие смены в `A80_CLOSED` с `close_reason=failed`, `failure_reason=no_performer_found`, `auto_closed=true`.

Для отчетов OMS3 есть read-only internal endpoint выборки смен по `starts_at`:

```http
GET /internal/shifts?startsAtFrom=2026-10-01T00:00:00Z&startsAtTo=2026-10-31T23:59:59Z
```

Endpoint возвращает смены внутри периода, отсортированные по `starts_at`, затем по `id`, и не публикует Kafka events.

## Offers

Одна смена имеет одного назначенного исполнителя. Предлагать смену можно нескольким исполнителям через offer campaign.

MVP-правило выбора исполнителя:

```text
first accepted wins
```

Правила public token endpoints:

- `GET /public/shift-offers/{token}` только показывает данные.
- `POST /public/shift-offers/{token}/viewed` логирует каждый просмотр, даже если offer уже недоступен.
- `POST /public/shift-offers/{token}/accept` является transactional command в рамках текущей реализации.
- Если смена уже занята, второй исполнитель получает `409 offer_lost`.
- `POST /public/shift-offers/{token}/decline` публикует событие отказа.

## Интеграция Kafka и RabbitMQ

OMS5 принимает people.performer.v1 и обновляет локальную проекцию исполнителя.

Факты operations.shift.assigned и operations.shift.completed публикуются в operations.performer.v1 через транзакционный outbox PostgreSQL с ключом Kafka person_id. Фоновый publisher повторяет отправку после ошибки.

OMS5 публикует operations.shift.status_changed напрямую после транзакции смены, с ключом shift_id и без outbox. OMS3 и OMS4 читают событие в независимых группах потребителей.

Остальные события OMS5 идут в отдельные топики с именами, совпадающими с event_type.

RabbitMQ передает фоновые команды. OMS5 не выполняет email- и report-задачи.

## API

### Health check

```http
GET /health
```

### Создать клиента

```http
POST /clients
Content-Type: application/json
```

Тело запроса:

```json
{
  "name": "ООО Клиент",
  "external_id": "CLIENT-0001"
}
```

### Создать смену

```http
POST /shifts
Content-Type: application/json
```

Тело запроса:

```json
{
  "client_id": "client-uuid",
  "starts_at": "2026-01-20T09:00:00Z",
  "ends_at": "2026-01-20T18:00:00Z",
  "location": "Москва"
}
```

Созданная смена получает статус:

```text
A00_DRAFT
```

### Перевести смену в новый статус

```http
POST /shifts/{shiftId}/transition
Content-Type: application/json
```

```json
{
  "new_status": "A10_CONFIRM",
  "reason": "confirmed by manager",
  "actor_user_id": "user-uuid"
}
```

### Синхронизировать исполнителя

OMS5 получает профиль из topic people.performer.v1. POST /performers/sync оставлен для внутренней синхронизации.

Пример тела:

```json
{
  "person_id": "1042",
  "display_name": "Иванов Иван",
  "status": "active",
  "source_version": 1,
  "engagement_type": "self_employed",
  "engagement_type_validation_status": "verified",
  "funnel_stage": "PERFORMER_READY_FOOD",
  "medical_book_status": "food",
  "completed_shifts_count": 0
}
```

Поля проекции nullable для старых записей. Прочитать профиль можно по person_id или performer_id:

```http
GET /performers/by-person/{person_id}
GET /performers/{performer_id}
```

### Создать кампанию предложений смены

Смена должна быть в статусе `A20_SOURCING`.

```http
POST /shifts/{shiftId}/offer-campaigns
Content-Type: application/json
```

```json
{
  "performer_ids": ["performer-uuid-1", "performer-uuid-2"]
}
```

Правило MVP:

```text
first accepted wins
```

### Public offer endpoints

```http
GET /public/shift-offers/{token}
POST /public/shift-offers/{token}/viewed
POST /public/shift-offers/{token}/accept
POST /public/shift-offers/{token}/decline
```

Accept возвращает снимок assignment.engagement_type. Публичный ответ не содержит token_hash и responded_at.

### Создать табельную запись

```http
POST /timesheets
Content-Type: application/json
```

Тело запроса:

```json
{
  "performer_id": "performer-uuid",
  "shift_id": "shift-uuid",
  "work_date": "2026-01-20",
  "hours": 8
}
```

### Получить смены для internal отчетов

```http
GET /internal/shifts?startsAtFrom=2026-10-01T00:00:00Z&startsAtTo=2026-10-31T23:59:59Z
```

Ответ:

```json
[
  {
    "id": "shift-uuid",
    "client_id": "client-uuid",
    "starts_at": "2026-10-05T09:00:00Z",
    "ends_at": "2026-10-05T18:00:00Z",
    "status": "A30_CHOICE",
    "location": "Москва",
    "assigned_performer_id": null,
    "close_reason": null,
    "failure_reason": null,
    "created_at": "2026-10-01T12:00:00Z",
    "updated_at": "2026-10-05T12:00:00Z"
  }
]
```

### Получить сводку

```http
GET /operations/summary
```

Ответ:

```json
{
  "clients": 1,
  "performers": 1,
  "shifts": 1,
  "offerCampaigns": 1,
  "offers": 2,
  "assignments": 1,
  "timesheets": 1
}
```

## PostgreSQL

Операционное состояние OMS5 хранится в PostgreSQL. При старте приложения выполняется идемпотентная миграция:

```text
app/migrations/001_create_operational_state.sql
app/migrations/002_performer_funnel_and_outbox.sql
```

Подключение задается переменной `DATABASE_URL`. В Kubernetes она передается через Secret. Для локального запуска:

```powershell
$env:DATABASE_URL = "postgresql://oms5:oms5@localhost:5435/oms5"
uvicorn app.main:app --host 0.0.0.0 --port 8005
```

Принятие предложения выполняется в транзакции PostgreSQL с блокировкой строки смены. Это обеспечивает правило `first accepted wins` при запросах из разных экземпляров OMS5. При недоступности PostgreSQL in-memory fallback не используется.

## Kafka events

- `operations.client_created` - создан клиент.
- `operations.performer.created` - создана projection исполнителя.
- `operations.performer.updated` - обновлена projection исполнителя.
- `operations.shift.created` - создана смена.
- `operations.shift.status_changed` - смена перешла в новый статус.
- `operations.shift.offer_campaign_created` - создана кампания предложений.
- `operations.shift.offer_sent` - предложение отправлено исполнителю.
- `operations.shift.offer_viewed` - предложение просмотрено.
- `operations.shift.offer_declined` - исполнитель отказался.
- `operations.shift.offer_won` - предложение победило.
- `operations.shift.offer_lost` - предложение проиграло, смена уже занята.
- `operations.shift.performer_assigned` - legacy-событие назначения, сохранено для совместимости.
- `operations.shift.assigned` и `operations.shift.completed` - event_type values in topic `operations.performer.v1`, ключ — `person_id`.
- `operations.timesheet.submitted` - табель отправлен.
- `operations.shift.closed` - смена закрыта.

### `operations.shift.status_changed`

`OMS5` публикует событие после каждого успешного валидного перехода статуса смены. Kafka key сообщения: `shift_id`.

```json
{
  "event_id": "uuid",
  "event_type": "operations.shift.status_changed",
  "schema_version": 1,
  "occurred_at": "2026-10-05T12:00:00Z",
  "correlation_id": "uuid",
  "producer": "OMS5",
  "shift_id": "uuid",
  "previous_status": "A20_SOURCING",
  "new_status": "A30_CHOICE",
  "reason": "performer_assigned"
}
```

Consumer-ы должны выполнять идемпотентность по `event_id`. `OMS3` помечает report cache смены как stale. `OMS4` принимает решение о необходимости уведомления.

## Переменные окружения

| Переменная | Значение по умолчанию | Назначение |
| --- | --- | --- |
| `SERVICE_NAME` | `OMS5` | Имя сервиса |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.oms.svc.cluster.local:9092` | Kafka bootstrap servers |
| `DATABASE_URL` | `postgresql://oms5:oms5@oms5-postgres:5432/oms5` | PostgreSQL connection string; в Kubernetes передается через Secret |
| `ENABLE_FUNNEL_OUTBOX` | `true` | Включает фоновый publisher; `0`, `false` и `no` отключают его |
| `FUNNEL_OUTBOX_BATCH_SIZE` | `100` | Размер пакета outbox |
| `FUNNEL_OUTBOX_POLL_INTERVAL` | `1.0` | Интервал опроса outbox в секундах |

## Локальный запуск

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8005
```

Встроенный Swagger UI FastAPI для OMS5:

```text
http://localhost:8005/docs
```

Это Swagger только текущего сервиса OMS5. Общая Swagger UI страница для единой спецификации `platform/contracts/openapi_oms_microservices.json` запускается отдельно из корня проекта и открывается без `/docs`:

```powershell
docker run --rm `
  --name oms-swagger-ui `
  -p 8088:8080 `
  -e SWAGGER_JSON=/spec/platform/contracts/openapi_oms_microservices.json `
  -v "D:/ProjectsDocker/extrawork:/spec" `
  swaggerapi/swagger-ui:v5.17.14
```

```text
http://localhost:8088
```

Через Ingress сервис доступен без port-forward. Встроенный Swagger UI OMS5:

```text
http://oms.local/oms5/docs
```

## Docker

```bash
docker build -t oms5:latest .
docker run --rm -p 8005:8000 oms5:latest
```

## Kubernetes

```bash
kubectl apply -f ../platform/k8s/namespace.yaml
kubectl apply -f k8s/
kubectl -n oms port-forward svc/oms5 8005:80
```

После port-forward встроенный Swagger UI OMS5 доступен по адресу:

```text
http://localhost:8005/docs
```
