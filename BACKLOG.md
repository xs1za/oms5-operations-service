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
