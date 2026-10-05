# OMS5 Backlog

- Заменить in-memory операционное состояние на БД с миграциями, транзакциями и блокировками для `first accepted wins`.
- Реализовать Kafka consumer для `employee.created`, `employee.updated`, `employee.deactivated` вместо ручного `/performers/sync`.
- Спроектировать и стабилизировать Kafka-событие изменения статуса смены в `OMS5`: контракт payload, версионирование, idempotency/correlation и потребителей.
- Добавить авторизацию через `OMS1` и разграничение ролей администратор/супервайзер/менеджер.
- Вынести public offer pages в `oms-portal-ui`; `OMS5` должен оставаться API-владельцем данных.
- Добавить `oms-admin-ui` для CRUD смен, подбора исполнителей, ручного назначения и проверки табеля.
- Интегрировать уведомления через `OMS4`: публиковать/инициировать шаблонные сценарии offer, assignment, absence и timesheet.
- Реализовать production auto-close через scheduled job/CronJob: закрывать незаполненные `A20_SOURCING` смены после закрытия отчетного периода.
- Доработать absence flow: `verification_required`, `verification_reason`, уведомление менеджера и ручное закрытие с `failure_reason=absence`.
- Для табеля добавить бизнес-правила: пересечения смен, лимиты часов, статусы согласования и проверку отчетного периода.
- Добавить event sync conflict handling для `Performer` projection.
- Добавить события `operations.shift.assignment_cancelled` и `operations.timesheet.verified`.
- Добавить расширенную аналитику просмотров offer pages.
- Добавить будущий конкурс между откликами исполнителей после MVP-правила `first accepted wins`.
