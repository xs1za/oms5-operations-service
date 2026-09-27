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

- Создание клиента.
- Создание смены по клиенту.
- Ведение FSM смены: `A00_DRAFT` -> `A90_ARCHIVE`.
- Синхронизация projection исполнителей из `OMS2.Employee`.
- Создание offer campaign и предложений смен исполнителям.
- Public token endpoints для `viewed`, `accept`, `decline`.
- Назначение первого откликнувшегося исполнителя на смену.
- Создание табельной записи по исполнителю и смене.
- Получение краткой сводки по операционным данным.
- Публикация доменных событий в Kafka.

## Технологии

- Python 3.11
- FastAPI
- Uvicorn
- in-memory хранилище для стартового прототипа
- confluent-kafka

## Термины

- `Shift` / смена - центральная бизнес-сущность OMS5.
- `Task` / задание - устаревший термин, больше не используется.
- `Performer` / исполнитель - локальная projection в OMS5, источником является `OMS2.Employee`.
- `User` / пользователь портала - аккаунт и роли в `OMS1`.

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

Endpoint имитирует consumer `employee.created` / `employee.updated` до реализации полноценного Kafka consumer.

```http
POST /performers/sync
Content-Type: application/json
```

```json
{
  "employee_id": 1,
  "display_name": "Иванов Иван",
  "email": "performer@example.com",
  "phone": "+79990000000",
  "status": "active",
  "source_version": 1
}
```

### Создать кампанию предложений смены

Смена должна быть в статусе `A20_SOURCING`.

```http
POST /shifts/{shiftId}/offer-campaigns
Content-Type: application/json
```

```json
{
  "performer_ids": ["performer-uuid-1", "performer-uuid-2"],
  "created_by_user_id": "manager-user-id"
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

`POST /viewed` логирует каждый просмотр, даже если offer уже недоступен. `POST /accept` является transactional command: если смена уже занята, второй исполнитель получает `409 offer_lost`.

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
- `operations.shift.performer_assigned` - исполнитель назначен.
- `operations.timesheet.submitted` - табель отправлен.
- `operations.shift.closed` - смена закрыта.

## Переменные окружения

| Переменная | Значение по умолчанию | Назначение |
| --- | --- | --- |
| `SERVICE_NAME` | `OMS5` | Имя сервиса |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.oms.svc.cluster.local:9092` | Kafka bootstrap servers |

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

## Важно для production

- Сейчас используется in-memory хранилище, данные теряются при рестарте.
- Нужно добавить БД, миграции и полноценные CRUD-операции.
- Нужно реализовать Kafka consumer для `employee.created`, `employee.updated`, `employee.deactivated`.
- Нужно добавить авторизацию через `OMS1`.
- Нужно вынести public offer pages в `oms-portal-ui`.
- Нужно добавить RabbitMQ-backed lightweight workers для report/email commands.
- Для табеля нужно добавить бизнес-правила: пересечения смен, лимиты часов, статусы согласования.
