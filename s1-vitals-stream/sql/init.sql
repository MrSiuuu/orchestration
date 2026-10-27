-- Schema HealthStream : table d'ingestion du flux mimic-vitals
CREATE TABLE IF NOT EXISTS vitals_raw (
    kafka_partition INTEGER       NOT NULL,
    kafka_offset    BIGINT        NOT NULL,
    patient_id      VARCHAR(16)   NOT NULL,
    event_time      TIMESTAMPTZ   NOT NULL,
    vital_type      VARCHAR(16)   NOT NULL,
    value           NUMERIC(8, 2) NOT NULL,
    unit            VARCHAR(8)    NOT NULL,
    ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    -- Rejouer le meme message Kafka ne peut pas creer une seconde ligne.
    CONSTRAINT pk_vitals_raw PRIMARY KEY (kafka_partition, kafka_offset),
    CONSTRAINT ck_vital_type CHECK (vital_type IN ('HR', 'SPO2', 'ABP_SYS'))
);

-- Requete dominante : la serie d'un patient sur une fenetre recente
CREATE INDEX IF NOT EXISTS idx_vitals_patient_time
    ON vitals_raw (patient_id, event_time DESC);

-- Vue de controle : preuve d'absence de doublon
CREATE OR REPLACE VIEW v_vitals_controle AS
SELECT COUNT(*)                                          AS lignes_totales,
       COUNT(DISTINCT (kafka_partition, kafka_offset))   AS messages_distincts,
       COUNT(DISTINCT patient_id)                        AS patients_distincts,
       MIN(event_time)                                   AS premier_releve,
       MAX(event_time)                                   AS dernier_releve
FROM vitals_raw;
