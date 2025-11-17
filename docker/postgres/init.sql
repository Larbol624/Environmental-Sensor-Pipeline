
-- this is the table for Raw_readings
CREATE TABLE IF NOT EXISTS public."Raw_readings"
(
    "Sensor_id" integer NOT NULL,
    "Timestamp" time without time zone NOT NULL,
    "Temperature" numeric(3,1),
    "Humidity" numeric(3,1),
    "Co2" integer
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Raw_readings"
    OWNER to "Larbol624";

COMMENT ON TABLE public."Raw_readings"
    IS 'This tables only takes in the raw_readings';


-- this the table for aggregated metrics
CREATE TABLE IF NOT EXISTS public.aggregated_metrics
(
    sensor_id integer NOT NULL,
    window_start time without time zone NOT NULL,
    avg_temp numeric(3,1),
    avg_humidity numeric(3,1),
    avg_co2 integer
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.aggregated_metrics
    OWNER to "Larbol624";

COMMENT ON TABLE public.aggregated_metrics
    IS 'Calculates the average of the temperature, humidity and co2';


-- this is the table for alerts
CREATE TABLE IF NOT EXISTS public.alerts
(
    sensor_id integer NOT NULL,
    "timestamp" time without time zone NOT NULL,
    type_alert "char" NOT NULL,
    message text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.alerts
    OWNER to "Larbol624";

COMMENT ON TABLE public.alerts
    IS 'This tabel contains alerts when the co2 or temperature reaches a certain treshold';