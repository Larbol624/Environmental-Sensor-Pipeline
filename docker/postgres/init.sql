-- Table: public.Raw_readings

-- DROP TABLE IF EXISTS public."Raw_readings";

CREATE TABLE IF NOT EXISTS public."Raw_readings"
(
    "Sensor_id" integer NOT NULL,
    "TimeStamp" timestamp(0) without time zone NOT NULL,
    "Temperature" numeric(4,1),
    "Humidity" numeric(4,1),
    "Co2" integer,
    CONSTRAINT "Raw_readings_pkey" PRIMARY KEY ("Sensor_id", "TimeStamp")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Raw_readings"
    OWNER to "Larbol624";

COMMENT ON TABLE public."Raw_readings"
    IS 'This table saves the raw_readings from the sensor data';


-- Table: public.Alerts

-- DROP TABLE IF EXISTS public."Alerts";

CREATE TABLE IF NOT EXISTS public."Alerts"
(
    "Sensor_id" integer NOT NULL,
    "TimeStamp" timestamp(0) without time zone NOT NULL,
    "error_Type" "char" NOT NULL,
    "Problem_message" text COLLATE pg_catalog."default",
    CONSTRAINT "Alerts_pkey" PRIMARY KEY ("Sensor_id", "TimeStamp", "error_Type")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Alerts"
    OWNER to "Larbol624";

COMMENT ON TABLE public."Alerts"
    IS 'This table contains all information about an alert if the temperature, Co2 level or Humidity is at
a critical level.
';


-- Table: public.Aggregated_metrics

-- DROP TABLE IF EXISTS public."Aggregated_metrics";

CREATE TABLE IF NOT EXISTS public."Aggregated_metrics"
(
    "Sensor_id" integer NOT NULL,
    "Window_start" timestamp(0) without time zone NOT NULL,
    "avg_Temp" numeric(4,1),
    "avg_Humidity" numeric(4,1),
    "avg_Co2" integer,
    CONSTRAINT "Aggregated_metrics_pkey" PRIMARY KEY ("Sensor_id", "Window_start")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Aggregated_metrics"
    OWNER to "Larbol624";

COMMENT ON TABLE public."Aggregated_metrics"
    IS 'This table contains info of the avg_temp, avg_humidity and Avg_Co2.
The table has a timewindow of 1 min.
So the window_start is the timestamp where the aggregating starts and then after 1 minute it ends.';