--Шаг 1. Подготовка среза для загрузки в целевую таблицу
DROP TABLE IF EXISTS catalog_name.schema_name.current_slice;
CREATE TABLE catalog_name.schema_name.current_slice AS 
WITH last_slice AS (
SELECT	
	customer_id,    
    mobile_phone_flg,
    email_flg,        
    sms_consent_flg,
    email_consent_flg,
    push_consent_flg,    
    LOWER(
	    TO_HEX(
            MD5(
                TO_UTF8(
                    CONCAT_WS(
                        '|',                        
                        CAST(customer_id as VARCHAR),
                        CAST(mobile_phone_flg as VARCHAR),
                        CAST(email_flg as VARCHAR),
                        CAST(sms_consent_flg as VARCHAR),
                        CAST(email_consent_flg as VARCHAR),
                        CAST(push_consent_flg as VARCHAR)                       
                    )
                )
            )
        )
    ) as hashdiff_key,
    CAST(CAST(dataflow_dttm AS DATE) AS TIMESTAMP) AS valid_from_dttm,
    CAST('5999-01-01 00:00:00' AS TIMESTAMP) AS valid_to_dttm
FROM
	catalog_name.schema_name.customer_consents
WHERE
	CAST({{ params.reload_flg }} AS VARCHAR) = '0'
)
, hist_slice AS (
SELECT
	customer_id,    
    mobile_phone_flg,
    email_flg,        
    sms_consent_flg,
    email_consent_flg,
    push_consent_flg,    
    LOWER(
	    TO_HEX(
            MD5(
                TO_UTF8(
                    CONCAT_WS(
                        '|',                        
                        CAST(customer_id as VARCHAR),
                        CAST(mobile_phone_flg as VARCHAR),
                        CAST(email_flg as VARCHAR),
                        CAST(sms_consent_flg as VARCHAR),
                        CAST(email_consent_flg as VARCHAR),
                        CAST(push_consent_flg as VARCHAR)                       
                    )
                )
            )
        )
    ) as hashdiff_key,
    CAST(dataflow_dt AS TIMESTAMP) AS valid_from_dttm,
    CAST('5999-01-01 00:00:00' AS TIMESTAMP) AS valid_to_dttm
FROM
	catalog_name.schema_name.customer_consents_sliced
WHERE
	CAST({{ params.reload_flg }} AS VARCHAR) = '1'
	AND dataflow_dt = CAST('{{ params.custom_dt }}' AS DATE)
) 
SELECT
	customer_id,    
    mobile_phone_flg,
    email_flg,        
    sms_consent_flg,
    email_consent_flg,
    push_consent_flg,    	
    hashdiff_key,
    valid_from_dttm,
    valid_to_dttm
FROM
    last_slice
UNION ALL
SELECT
    customer_id,    
    mobile_phone_flg,
    email_flg,        
    sms_consent_flg,
    email_consent_flg,
    push_consent_flg,    	
    hashdiff_key,
    valid_from_dttm,
    valid_to_dttm
FROM
    hist_slice
;

--Шаг 2. Подготовка среза предыдущего состояния
DROP TABLE IF EXISTS catalog_name.schema_name.previous_slice;
CREATE TABLE catalog_name.schema_name.previous_slice AS 
SELECT
	customer_id,    
    mobile_phone_flg,
    email_flg,        
    sms_consent_flg,
    email_consent_flg,
    push_consent_flg,    
    LOWER(
	    TO_HEX(
            MD5(
                TO_UTF8(
                    CONCAT_WS(
                        '|',                        
                        CAST(customer_id AS VARCHAR),
                        CAST(mobile_phone_flg AS VARCHAR),
                        CAST(email_flg AS VARCHAR),
                        CAST(sms_consent_flg AS VARCHAR),
                        CAST(email_consent_flg AS VARCHAR),
                        CAST(push_consent_flg AS VARCHAR)                       
                    )
                )
            )
        )
    ) AS hashdiff_key,
    valid_from_dttm,
    valid_to_dttm
FROM
	catalog_name.schema_name.customer_consents_versioned
WHERE
	valid_to_dttm = CAST('5999-01-01 00:00:00' AS TIMESTAMP)
;

--Шаг 3. Новые записи относительно снимка предыдущего состояния
DROP TABLE IF EXISTS catalog_name.schema_name.appended_rows;
CREATE TABLE catalog_name.schema_name.appended_rows AS
SELECT
	cs.customer_id,    
    cs.mobile_phone_flg,
	cs.email_flg,        
    cs.sms_consent_flg,
    cs.email_consent_flg,
    cs.push_consent_flg,
    cs.valid_from_dttm,
    cs.valid_to_dttm
FROM
	catalog_name.schema_name.current_slice cs
LEFT JOIN
	catalog_name.schema_name.previous_slice ps ON cs.customer_id = ps.customer_id
WHERE
	ps.customer_id IS NULL
;
	

--Шаг 4. Удаленные записи
DROP TABLE IF EXISTS catalog_name.schema_name.deleted_rows;
CREATE TABLE catalog_name.schema_name.deleted_rows AS
SELECT
	ps.customer_id,    
    ps.mobile_phone_flg,
	ps.email_flg,        
    ps.sms_consent_flg,
    ps.email_consent_flg,
    ps.push_consent_flg,
    ps.valid_from_dttm,
    cj.valid_to_dttm
FROM
	catalog_name.schema_name.previous_slice ps
LEFT JOIN
	catalog_name.schema_name.current_slice cs ON ps.customer_id = cs.customer_id
CROSS JOIN
	(
		SELECT valid_from_dttm AS valid_to_dttm
		FROM catalog_name.schema_name.current_slice
		LIMIT 1
	) cj
WHERE
	cs.customer_id IS NULL
;
select *
from catalog_name.schema_name.deleted_rows 

--Шаг 5. Изменившиеся записи для добавления
DROP TABLE IF EXISTS catalog_name.schema_name.updated_rows;
CREATE TABLE catalog_name.schema_name.updated_rows AS
SELECT
	cs.customer_id,    
    cs.mobile_phone_flg,
	cs.email_flg,        
    cs.sms_consent_flg,
    cs.email_consent_flg,
    cs.push_consent_flg,
    cs.valid_from_dttm, 
    cs.valid_to_dttm
FROM
	catalog_name.schema_name.current_slice cs
LEFT JOIN
	catalog_name.schema_name.previous_slice ps ON cs.customer_id = ps.customer_id
WHERE
	ps.hashdiff_key != cs.hashdiff_key
;

--Шаг 6. Изменившиеся записи для закрытия (обновление valid_to_dttm) в целевой таблице
DROP TABLE IF EXISTS catalog_name.schema_name.closed_rows;
CREATE TABLE catalog_name.schema_name.closed_rows AS
SELECT
	ps.customer_id,    
    ps.mobile_phone_flg,
	ps.email_flg,        
    ps.sms_consent_flg,
    ps.email_consent_flg,
    ps.push_consent_flg,
    ps.valid_from_dttm,
    cs.valid_from_dttm AS valid_to_dttm
FROM
	catalog_name.schema_name.previous_slice ps
LEFT JOIN
	catalog_name.schema_name.current_slice cs ON ps.customer_id = cs.customer_id
WHERE
	ps.hashdiff_key != cs.hashdiff_key
;

--Шаг 7. Сборка всех записей для вставки в целевую таблицу
DROP TABLE IF EXISTS catalog_name.schema_name.load_batch;
CREATE TABLE catalog_name.schema_name.load_batch AS
SELECT
	customer_id,    
    mobile_phone_flg,
    email_flg,        
    sms_consent_flg,
    email_consent_flg,
    push_consent_flg,
    valid_from_dttm,
    valid_to_dttm
FROM 
	catalog_name.schema_name.appended_rows
UNION ALL
SELECT
	customer_id,    
    mobile_phone_flg,
    email_flg,        
    sms_consent_flg,
    email_consent_flg,
    push_consent_flg,
    valid_from_dttm,
    valid_to_dttm
FROM 
	catalog_name.schema_name.deleted_rows
UNION ALL
SELECT
	customer_id,    
    mobile_phone_flg,
    email_flg,        
    sms_consent_flg,
    email_consent_flg,
    push_consent_flg,
    valid_from_dttm,
    valid_to_dttm
FROM 
	catalog_name.schema_name.updated_rows
UNION ALL
SELECT
	customer_id,    
    mobile_phone_flg,
    email_flg,        
    sms_consent_flg,
    email_consent_flg,
    push_consent_flg,
    valid_from_dttm,
    valid_to_dttm
FROM 
	catalog_name.schema_name.closed_rows
;

select * 
from catalog_name.schema_name.load_batch;
	
	
--Шаг 8. Merge в целевую таблицу
MERGE INTO catalog_name.schema_name.customer_consents_versioned AS trg
USING catalog_name.schema_name.load_batch src
	ON src.customer_id = trg.customer_id
	AND src.valid_from_dttm = trg.valid_from_dttm
WHEN MATCHED AND (    
    src.mobile_phone_flg IS DISTINCT FROM trg.mobile_phone_flg
    OR src.email_flg IS DISTINCT FROM trg.email_flg
    OR src.sms_consent_flg IS DISTINCT FROM trg.sms_consent_flg
    OR src.email_consent_flg IS DISTINCT FROM trg.email_consent_flg
    OR src.push_consent_flg IS DISTINCT FROM trg.push_consent_flg
    OR src.valid_to_dttm IS DISTINCT FROM trg.valid_to_dttm
)
THEN UPDATE SET    
	mobile_phone_flg = src.mobile_phone_flg,
	email_flg = src.email_flg,        
	sms_consent_flg = src.sms_consent_flg,
	email_consent_flg = src.email_consent_flg,
	push_consent_flg = src.push_consent_flg,
	valid_to_dttm = src.valid_to_dttm,
	dataflow_dttm = DATE_TRUNC('SECOND', CURRENT_TIMESTAMP)
WHEN NOT MATCHED THEN INSERT (
	customer_id,    
    mobile_phone_flg,
    email_flg,        
    sms_consent_flg,
    email_consent_flg,
    push_consent_flg,
    valid_from_dttm,
    valid_to_dttm,
    dataflow_dttm
) 
VALUES (	
	src.customer_id,    
    src.mobile_phone_flg,
    src.email_flg,        
    src.sms_consent_flg,
    src.email_consent_flg,
    src.push_consent_flg,
    src.valid_from_dttm,
    src.valid_to_dttm,
	DATE_TRUNC('SECOND', CURRENT_TIMESTAMP)
);
















	