--Create tables on Trino SQL
DROP TABLE IF EXISTS catalog_name.schema_name.customer_consents;
CREATE TABLE catalog_name.schema_name.customer_consents
(
    customer_id bigint,
    mobile_phone_flg boolean,
    email_flg boolean,
    sms_consent_flg boolean,
    email_consent_flg boolean,
    push_consent_flg boolean,
    dataflow_dttm timestamp
)
WITH (partitioning = ARRAY['bucket(customer_id, 5)']);


DROP TABLE IF EXISTS catalog_name.schema_name.customer_consents_sliced;
CREATE TABLE catalog_name.schema_name.customer_consents_sliced
(
    customer_id bigint,
    mobile_phone_flg boolean,
    email_flg boolean,
    sms_consent_flg boolean,
    email_consent_flg boolean,
    push_consent_flg boolean,
	dataflow_dt date,
    dataflow_dttm timestamp
)
WITH (partitioning = ARRAY['dataflow_dt', 'bucket(customer_id, 5)']);


DROP TABLE IF EXISTS catalog_name.schema_name.customer_consents_versioned;
CREATE TABLE catalog_name.schema_name.customer_consents_versioned
(
    customer_id bigint,
    mobile_phone_flg boolean,
    email_flg boolean,
    sms_consent_flg boolean,
    email_consent_flg boolean,
    push_consent_flg boolean,
    valid_from_dttm timestamp,
    valid_to_dttm timestamp,
    dataflow_dttm timestamp
)
WITH (partitioning = ARRAY['bucket(customer_id, 5)']);