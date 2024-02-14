CREATE TABLE IF NOT EXISTS pmt_dpl.dim_authorization_status
(
   src_sys_code                      varchar(10),
   authorization_status_code         varchar(30),
   authorization_status_description  varchar(100),
   unqid                             varchar(40),
   dbt_scd_id                        varchar(100),
   dbt_updated_at                    timestamp,
   dbt_valid_from                    timestamp,
   dbt_valid_to                      timestamp
);

GRANT REFERENCES, INSERT, TRIGGER, DELETE, TRUNCATE, SELECT, UPDATE ON pmt_dpl.dim_authorization_status TO pmt_write_role;
GRANT SELECT ON pmt_dpl.dim_authorization_status TO pmt_read_role;

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_business_area
(
   src_sys_code               varchar(10),
   business_area_code         varchar(10),
   business_area_description  varchar(75),
   unqid                      varchar(20),
   dbt_scd_id                 varchar(100),
   dbt_updated_at             timestamp,
   dbt_valid_from             timestamp,
   dbt_valid_to               timestamp
);

GRANT REFERENCES, INSERT, TRIGGER, DELETE, TRUNCATE, SELECT, UPDATE ON pmt_dpl.dim_business_area TO pmt_write_role;
GRANT SELECT ON pmt_dpl.dim_business_area TO pmt_read_role;

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_location
(
   src_sys_code    varchar(20),
   location_code   varchar(200),
   project_code    varchar(125),
   unqid           varchar(220),
   dbt_scd_id      varchar(100),
   dbt_updated_at  timestamp,
   dbt_valid_from  timestamp,
   dbt_valid_to    timestamp
);

GRANT INSERT, DELETE, UPDATE, SELECT ON pmt_dpl.dim_location TO pmt_write_role;
GRANT SELECT ON pmt_dpl.dim_location TO pmt_read_role;

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_ministry
(
   src_sys_code          varchar(10),
   ministry_code         varchar(10),
   ministry_description  varchar(75),
   dbt_scd_id            varchar(100),
   dbt_updated_at        timestamp,
   dbt_valid_from        timestamp,
   dbt_valid_to          timestamp
);

GRANT REFERENCES, INSERT, TRIGGER, DELETE, TRUNCATE, SELECT, UPDATE ON pmt_dpl.dim_ministry TO pmt_write_role;
GRANT SELECT ON pmt_dpl.dim_ministry TO pmt_read_role;

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_org
(
   src_sys_code                 varchar(10),
   org_unit_code                varchar(6),
   org_unit_description         varchar(100),
   rollup_region_code           varchar(6),
   rollup_region_description    varchar(100),
   rollup_district_code         varchar(6),
   rollup_district_description  varchar(100),
   roll_up_area_code            varchar(10),
   roll_up_area_description     varchar(100),
   unqid                        varchar(20),
   dbt_scd_id                   varchar(100),
   dbt_updated_at               timestamp,
   dbt_valid_from               timestamp,
   dbt_valid_to                 timestamp
);

GRANT REFERENCES, INSERT, TRIGGER, DELETE, TRUNCATE, SELECT, UPDATE ON pmt_dpl.dim_org TO pmt_write_role;
GRANT SELECT ON pmt_dpl.dim_org TO pmt_read_role;

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_permit_status
(
   src_sys_code               varchar(10),
   permit_status_code         varchar(30),
   permit_status_description  varchar(100),
   unqid                      varchar(40),
   dbt_scd_id                 varchar(100),
   dbt_updated_at             timestamp,
   dbt_valid_from             timestamp,
   dbt_valid_to               timestamp
);

GRANT REFERENCES, INSERT, TRIGGER, DELETE, TRUNCATE, SELECT, UPDATE ON pmt_dpl.dim_permit_status TO pmt_write_role;
GRANT SELECT ON pmt_dpl.dim_permit_status TO pmt_read_role;

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_permit_type
(
   src_sys_code             varchar(10),
   permit_type_code         varchar(30),
   permit_type_description  varchar(120),
   unqid                    varchar(30),
   dbt_scd_id               varchar(100),
   dbt_updated_at           timestamp,
   dbt_valid_from           timestamp,
   dbt_valid_to             timestamp
);

GRANT REFERENCES, INSERT, TRIGGER, DELETE, TRUNCATE, SELECT, UPDATE ON pmt_dpl.dim_permit_type TO pmt_write_role;
GRANT SELECT ON pmt_dpl.dim_permit_type TO pmt_read_role;

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_project
(
   src_sys_code                varchar(20),
   project_id                  varchar(20),
   project_name                varchar(125),
   project_location_code       varchar(200),
   project_status_code         varchar(6),
   project_region_description  varchar(125),
   unqid                       varchar(30),
   dbt_scd_id                  varchar(100),
   dbt_updated_at              timestamp,
   dbt_valid_from              timestamp,
   dbt_valid_to                timestamp
);

GRANT INSERT, DELETE, UPDATE, SELECT ON pmt_dpl.dim_project TO pmt_write_role;
GRANT SELECT ON pmt_dpl.dim_project TO pmt_read_role;

CREATE TABLE IF NOT EXISTS pmt_dpl.dim_source_system
(
   src_sys_code         varchar(20),
   src_sys_description  varchar(100),
   unqid                varchar(20),
   dbt_scd_id           varchar(100),
   dbt_updated_at       timestamp,
   dbt_valid_from       timestamp,
   dbt_valid_to         timestamp
);

GRANT INSERT, DELETE, UPDATE, SELECT ON pmt_dpl.dim_source_system TO pmt_write_role;
GRANT SELECT ON pmt_dpl.dim_source_system TO pmt_read_role;

CREATE TABLE IF NOT EXISTS pmt_dpl.fact_permits
(
   src_sys_code               varchar(10),
   ministry_code              varchar(10),
   business_area_code         varchar(10),
   permit_id                  varchar(10),
   app_file_id                varchar(45),
   authorization_status_code  varchar(30),
   project_id                 varchar(20),
   permit_status_code         varchar(30),
   permit_status_date         timestamp,
   permit_issue_date          timestamp,
   permit_issue_expire_date   timestamp,
   app_received_date          timestamp,
   app_update_date            timestamp,
   app_decision_date          timestamp,
   app_issuance_date          timestamp,
   app_accepted_date          timestamp,
   app_rejected_date          timestamp,
   app_adjudication_date      timestamp,
   harvest_auth_status        varchar(20),
   harvest_area_sq_m          integer,
   permit_org_unit_code       varchar(6),
   application_age            numeric,
   map_feature_id             bigint,
   permit_type_code           varchar(30),
   unqid                      varchar(75),
   dbt_scd_id                 varchar(100),
   dbt_updated_at             timestamp,
   dbt_valid_from             timestamp,
   dbt_valid_to               timestamp
);

GRANT REFERENCES, INSERT, TRIGGER, DELETE, TRUNCATE, SELECT, UPDATE ON pmt_dpl.fact_permits TO pmt_write_role;
GRANT SELECT ON pmt_dpl.fact_permits TO pmt_read_role;