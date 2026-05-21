-- Crear base de dades / esquema
CREATE DATABASE IF NOT EXISTS metadata_demo;

USE metadata_demo;

-- 1) Taula de dominis de dades
CREATE TABLE IF NOT EXISTS data_domains (
  domain_id        INT,
  name             STRING,
  owner_group      STRING,
  description      STRING,
  criticality      STRING,    -- LOW / MEDIUM / HIGH
  created_at       TIMESTAMP,
  updated_at       TIMESTAMP
)
USING iceberg;

INSERT INTO data_domains VALUES
  (1, 'Finance',   'data-finance',   'Dades financeres i facturació',              'HIGH',   current_timestamp(), current_timestamp()),
  (2, 'Marketing', 'data-marketing', 'Leads, campanyes i mètriques de màrqueting', 'MEDIUM', current_timestamp(), current_timestamp()),
  (3, 'Product',   'data-product',   'Ús de producte i feature flags',             'HIGH',   current_timestamp(), current_timestamp()),
  (4, 'HR',        'data-hr',        'Dades de persones i organització',           'HIGH',   current_timestamp(), current_timestamp());

-- 2) Taula d’usuaris / stewards
CREATE TABLE IF NOT EXISTS users (
  user_id          INT,
  username         STRING,
  full_name        STRING,
  email            STRING,
  role             STRING,     -- DATA_ENGINEER / ANALYST / STEWARD / OWNER
  department       STRING,
  is_active        BOOLEAN,
  created_at       TIMESTAMP,
  last_login_at    TIMESTAMP
)
USING iceberg;

INSERT INTO users VALUES
  (1, 'marta.de',   'Marta Delgado',   'marta.de@example.com',   'DATA_ENGINEER', 'Data Platform', true,  current_timestamp(), current_timestamp()),
  (2, 'jordi.an',   'Jordi Anglès',    'jordi.an@example.com',   'ANALYST',       'Finance',       true,  current_timestamp(), current_timestamp()),
  (3, 'laia.ru',    'Laia Rubio',      'laia.ru@example.com',    'STEWARD',       'Marketing',     true,  current_timestamp(), current_timestamp()),
  (4, 'pere.ro',    'Pere Ros',        'pere.ro@example.com',    'OWNER',         'Product',       true,  current_timestamp(), current_timestamp()),
  (5, 'ana.hr',     'Ana Herrera',     'ana.hr@example.com',     'STEWARD',       'HR',            false, current_timestamp(), null);

-- 3) Taula de datasets (pensada per mapping a DataHub Datasets)
CREATE TABLE IF NOT EXISTS datasets (
  dataset_id       INT,
  name             STRING,      -- short name (p.e. 'finance_invoices')
  display_name     STRING,      -- nom llegible
  domain_id        INT,         -- FK -> data_domains.domain_id
  owner_user_id    INT,         -- FK -> users.user_id
  storage_system   STRING,      -- ICEBERG / DELTA / EXTERNAL / VIEW
  platform         STRING,      -- 'spark', 'mysql', 'kafka', etc.
  physical_path    STRING,      -- p.e. 's3a://warehouse/finance/invoices'
  lifecycle_stage  STRING,      -- RAW / CURATED / MART
  pii_level        STRING,      -- NONE / LOW / MEDIUM / HIGH
  row_count        BIGINT,
  last_refreshed_at TIMESTAMP,
  created_at       TIMESTAMP
)
USING iceberg;

INSERT INTO datasets VALUES
  (1, 'finance_invoices', 'Finance - Invoices', 1, 1,
   'ICEBERG', 'spark', 's3a://warehouse/finance/invoices',
   'CURATED', 'MEDIUM', 120000, current_timestamp(), date_sub(current_timestamp(), 7)),
  (2, 'finance_payments', 'Finance - Payments', 1, 1,
   'ICEBERG', 'spark', 's3a://warehouse/finance/payments',
   'CURATED', 'HIGH', 80000, current_timestamp(), date_sub(current_timestamp(), 2)),
  (3, 'mkt_campaign_events', 'Marketing - Campaign Events', 2, 3,
   'ICEBERG', 'spark', 's3a://warehouse/marketing/campaign_events',
   'RAW', 'LOW', 1500000, current_timestamp(), date_sub(current_timestamp(), 1)),
  (4, 'product_feature_usage', 'Product - Feature Usage', 3, 4,
   'ICEBERG', 'spark', 's3a://warehouse/product/feature_usage',
   'CURATED', 'LOW', 5600000, current_timestamp(), date_sub(current_timestamp(), 3)),
  (5, 'hr_employee_master', 'HR - Employee Master', 4, 5,
   'ICEBERG', 'spark', 's3a://warehouse/hr/employee_master',
   'CURATED', 'HIGH', 3200, current_timestamp(), date_sub(current_timestamp(), 10));

-- 4) Taula de pipelines (job definitions)
CREATE TABLE IF NOT EXISTS pipelines (
  pipeline_id      INT,
  name             STRING,      -- p.e. 'finance_invoices_daily'
  description      STRING,
  owner_user_id    INT,
  source_dataset_id INT,        -- pot ser null si el pipeline ingereix d'un sistema extern
  target_dataset_id INT,
  schedule         STRING,      -- CRON o etiqueta ('DAILY', 'HOURLY', ...)
  status           STRING,      -- ACTIVE / PAUSED / FAILED
  last_run_at      TIMESTAMP,
  last_run_status  STRING,      -- SUCCESS / FAILED / PARTIAL
  created_at       TIMESTAMP
)
USING iceberg;

INSERT INTO pipelines VALUES
  (1, 'finance_invoices_daily',
   'Ingesta diària de factures des de l’ERP a la taula curated',
   1, null, 1, '0 3 * * *', 'ACTIVE', date_sub(current_timestamp(), 1), 'SUCCESS', date_sub(current_timestamp(), 60)),
  (2, 'finance_payments_daily',
   'Ingesta diària de pagaments i reconciliació amb factures',
   1, 1, 2, '0 4 * * *', 'ACTIVE', date_sub(current_timestamp(), 1), 'FAILED', date_sub(current_timestamp(), 30)),
  (3, 'mkt_campaign_events_stream',
   'Stream de events de campanya des de Kafka a raw',
   3, null, 3, '@hourly', 'ACTIVE', date_sub(current_timestamp(), 0), 'SUCCESS', date_sub(current_timestamp(), 5)),
  (4, 'product_feature_usage_agg',
   'Agregació diària d’ús de funcionalitats per dashboard de producte',
   4, 3, 4, '0 5 * * *', 'PAUSED', date_sub(current_timestamp(), 10), 'SUCCESS', date_sub(current_timestamp(), 120)),
  (5, 'hr_employee_sync',
   'Sincronització setmanal del master d’empleats',
   5, null, 5, '0 2 * * 1', 'ACTIVE', date_sub(current_timestamp(), 5), 'SUCCESS', date_sub(current_timestamp(), 365));

-- 5) Taula de dashboards / reports
CREATE TABLE IF NOT EXISTS dashboards (
  dashboard_id     INT,
  name             STRING,
  display_name     STRING,
  owner_user_id    INT,
  main_domain_id   INT,
  tool             STRING,      -- 'powerbi', 'superset', 'tableau', ...
  url              STRING,
  is_active        BOOLEAN,
  primary_dataset_id INT,       -- dataset principal
  created_at       TIMESTAMP,
  last_viewed_at   TIMESTAMP,
  weekly_view_count INT
)
USING iceberg;

INSERT INTO dashboards VALUES
  (1, 'finance_executive_overview', 'Finance - Executive Overview',
   2, 1, 'superset', 'https://bi.example.com/superset/dashboard/finance_exec',
   true, 1, date_sub(current_timestamp(), 200), date_sub(current_timestamp(), 1), 120),
  (2, 'marketing_campaign_performance', 'Marketing - Campaign Performance',
   3, 2, 'superset', 'https://bi.example.com/superset/dashboard/mkt_campaigns',
   true, 3, date_sub(current_timestamp(), 150), date_sub(current_timestamp(), 0), 340),
  (3, 'product_feature_adoption', 'Product - Feature Adoption',
   4, 3, 'superset', 'https://bi.example.com/superset/dashboard/feature_adoption',
   true, 4, date_sub(current_timestamp(), 180), date_sub(current_timestamp(), 2), 210),
  (4, 'hr_headcount_trends', 'HR - Headcount Trends',
   5, 4, 'superset', 'https://bi.example.com/superset/dashboard/hr_headcount',
   false, 5, date_sub(current_timestamp(), 300), date_sub(current_timestamp(), 40), 35),
  (5, 'finance_cashflow', 'Finance - Cashflow Overview',
   2, 1, 'superset', 'https://bi.example.com/superset/dashboard/finance_cashflow',
   true, 2, date_sub(current_timestamp(), 90), date_sub(current_timestamp(), 3), 75);

-- 6) (Opcional però útil per a lineage): enllaç datasets <-> dashboards (molts a molts)
CREATE TABLE IF NOT EXISTS dashboard_datasets (
  dashboard_id     INT,
  dataset_id       INT,
  usage_type       STRING,     -- PRIMARY / SECONDARY / FILTER_ONLY
  added_at         TIMESTAMP
)
USING iceberg;

INSERT INTO dashboard_datasets VALUES
  (1, 1, 'PRIMARY', current_timestamp()),
  (1, 2, 'SECONDARY', current_timestamp()),
  (2, 3, 'PRIMARY', current_timestamp()),
  (3, 4, 'PRIMARY', current_timestamp()),
  (4, 5, 'PRIMARY', current_timestamp()),
  (5, 1, 'SECONDARY', current_timestamp()),
  (5, 2, 'PRIMARY', current_timestamp());