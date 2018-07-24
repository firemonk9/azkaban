CREATE TABLE job_dv_results (
  exec_id    INT          NOT NULL,
  dv_id      INT          NOT NULL,
  project_id   INT NOT NULL,
  flow_id   VARCHAR(512) NOT NULL,
  job_id   VARCHAR(512) NOT NULL,
  check_time BIGINT       NOT NULL,
  result       BOOL      NOT NULL,
  result_value   BIGINT    ,
  expected_value   BIGINT    ,
  PRIMARY KEY (exec_id,dv_id)
);
