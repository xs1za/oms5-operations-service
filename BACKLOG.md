# OMS5 Backlog

- Подготовить oms-portal-ui для public offer pages; OMS5 остается API-владельцем данных.
- Подготовить oms-admin-ui для смен, исполнителей, назначений и табелей.
- Интегрировать OMS4 для шаблонных offer, assignment, absence и timesheet уведомлений.
- Добавить production CronJob для auto-close незаполненных A20_SOURCING после закрытия отчетного периода.
- Добавить absence review flow с verification_required, verification_reason и уведомлением менеджера; generic closure уже поддерживает failure_reason=absence.
- Добавить проверки пересечения смен, согласования и отчетного периода для timesheets; сейчас hours ограничен диапазоном 0 < hours <= 24.
- Публиковать operations.shift.assignment_cancelled и operations.timesheet.verified.
- Добавить аналитику просмотров offer pages.
- Спроектировать конкурс между откликами исполнителей после MVP first accepted wins.
- Привести доступ `GET /operations/summary` в соответствие с контрактом и оформить отдельную задачу (предлагаемый код `OMS5-SEC-0001`; при правке контракта — парная `DOC-API`): контракт `platform/contracts/openapi_oms_microservices.json` объявляет `security: bearerAuth` с ответами `401`/`403`, а обработчик `summary()` в `app/main.py` не имеет зависимостей доступа — endpoint открыт. Известные требования: подтвердить модель доступа (ожидаемо операторский доступ как у endpoints смен: `require_principal` + `require_operator`, реальные `401 invalid_token` и `403 operator_role_required` — тогда контракт не меняется, добавляются зависимость и тесты); если endpoint задуман публичным — изменить контракт по SemVer и убрать `security`/`401`/`403`. Найдено 07.11.2026 при проработке `DOC-API-0003`.
