-- Table: public.Alerts

-- DROP TABLE IF EXISTS public."Alerts";

CREATE TABLE IF NOT EXISTS public.alerts
(
    sensor_id integer,
    "timestamp" timestamp without time zone,
    temperature double precision,
    humidity double precision,
    co2 integer,
    kafka_timestamp timestamp without time zone,
    alert_reason character varying(32) COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.alerts
    OWNER to "Larbol624";


-- Table: public.aggregated_metrics

-- DROP TABLE IF EXISTS public.aggregated_metrics;

CREATE TABLE IF NOT EXISTS public.aggregated_metrics
(
    sensor_id integer,
    "timestamp" timestamp without time zone,
    temperature double precision,
    humidity double precision,
    co2 integer,
    temp_delta double precision,
    humid_delta double precision,
    co2_delta integer,
    temp_anomaly boolean,
    humid_anomaly boolean,
    co2_anomaly boolean,
    anomaly boolean,
    ingest_ts timestamp without time zone NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.aggregated_metrics
    OWNER to "Larbol624";

