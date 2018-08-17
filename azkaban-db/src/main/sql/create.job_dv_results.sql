CREATE TABLE job_dv_results (
  project_id   INT NOT NULL,
  exec_id    INT          NOT NULL,
  job_id   VARCHAR(512) NOT NULL,
  check_time BIGINT       NOT NULL,
  result       BOOL      NOT NULL,
  result_value   BIGINT    ,
  expected_value   BIGINT    ,
  job_status VARCHAR(512) NOT NULL,
  PRIMARY KEY (project_id,exec_id)
);
