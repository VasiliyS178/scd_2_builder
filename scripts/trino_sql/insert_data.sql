--Итерация 1. Первоначальный срез данных для наполения целевой таблицы
DELETE FROM catalog_name.schema_name.customer_consents;
INSERT INTO catalog_name.schema_name.customer_consents
(customer_id, mobile_phone_flg, email_flg, sms_consent_flg, email_consent_flg, push_consent_flg, dataflow_dttm) VALUES
(13866724, true, true, true, true, true, timestamp'2026-03-17 14:42:38'),
(54046421, true, true, false, false, false, timestamp'2026-03-17 14:42:38'),
(12396421, true, true, true, false, false, timestamp'2026-03-17 14:42:38'),
(95296428, true, false, true, false, false, timestamp'2026-03-17 14:42:38')
;

--Итерация 2. Проверка добавления и удаления записей
--Добавлена новая запись: 99999123, удалена запись 95296428. Остальные - без изменений
DELETE FROM catalog_name.schema_name.customer_consents;
INSERT INTO catalog_name.schema_name.customer_consents
(customer_id, mobile_phone_flg, email_flg, sms_consent_flg, email_consent_flg, push_consent_flg, dataflow_dttm) VALUES
(99999123, true, true, true, true, true, timestamp'2026-03-23 10:42:38'),
(13866724, true, true, true, true, true, timestamp'2026-03-23 10:42:38'),
(54046421, true, true, false, false, false, timestamp'2026-03-23 10:42:38'),
(12396421, true, true, true, false, false, timestamp'2026-03-23 10:42:38')
;

--Итерация 3. Проверка обновления и закрытия записей
--Изменен флаг email_consent_flg на FALSE у 13866724, изменен флаг sms_consent_flg на TRUE у 54046421
DELETE FROM catalog_name.schema_name.customer_consents;
INSERT INTO catalog_name.schema_name.customer_consents
(customer_id, mobile_phone_flg, email_flg, sms_consent_flg, email_consent_flg, push_consent_flg, dataflow_dttm) VALUES
(99999123, true, true, true, true, true, timestamp'2026-03-26 10:42:38'),
(13866724, true, true, true, false, true, timestamp'2026-03-26 10:42:38'),
(54046421, true, true, true, false, false, timestamp'2026-03-26 10:42:38'),
(12396421, true, true, true, false, false, timestamp'2026-03-26 10:42:38')
;