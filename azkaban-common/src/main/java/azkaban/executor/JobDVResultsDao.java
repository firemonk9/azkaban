/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.executor;

import azkaban.db.DatabaseOperator;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Singleton
public class JobDVResultsDao {

    private static final Logger logger = Logger.getLogger(ExecutorDao.class);
    private final DatabaseOperator dbOperator;

    @Inject
    JobDVResultsDao(final DatabaseOperator databaseOperator) {
        this.dbOperator = databaseOperator;
    }

    public void insertJobResults(int projectId, int execid, String jobId,  Long resultCount, Long expectedCount)
            throws ExecutorManagerException {

//1, 0, 1, shell_end, null, 1532377898291, PASS, 1
        logger.info("Uploading flowId " + jobId);
        try {
            this.dbOperator.update(INSERT_EXECUTION_NODE, projectId,
                    execid, jobId,
                    System.currentTimeMillis(),
                    true, resultCount,expectedCount,"COMPLETED");
        } catch (final SQLException e) {
            throw new ExecutorManagerException("Error writing job " + 0, e);
        }
    }


    public List<JobDVResults> fetchJobResults(final int projectId)
            throws ExecutorManagerException {

        try {
            return this.dbOperator.query(SELECT_EXECUTOR_RESULTS, new JobDVResultsHandler(), projectId);
        } catch (final SQLException e) {
            throw new ExecutorManagerException("Error fetching num executions", e);
        }
    }

    public List<JobDVResults> fetchJobResults(final int projectId,final Integer exec_id)
            throws ExecutorManagerException {

        try {
            if(exec_id == null)
                return fetchJobResults(projectId);
            return this.dbOperator.query(SELECT_EXECUTOR_RESULTS_FLOW, new JobDVResultsHandler(), projectId, exec_id);
        } catch (final SQLException e) {
            throw new ExecutorManagerException("Error fetching num executions", e);
        }
    }


    /**
     * JDBC ResultSetHandler to fetch records from executor_events table
     */
    private static class JobDVResultsHandler implements
            ResultSetHandler<List<JobDVResults>> {


        @Override
        public List<JobDVResults> handle(final ResultSet rs) throws SQLException {
            if (!rs.next()) {
                return Collections.<JobDVResults>emptyList();
            }

            final ArrayList<JobDVResults> events = new ArrayList<>();
            do {
                final int project_id = rs.getInt(1);
                final int exec_id = rs.getInt(2);
                final String job_id = rs.getString(3);
                final Long check_time = rs.getLong(4);
                final String result = rs.getString(5);
                final Long result_value = rs.getLong(6);
                final Long expected_value = rs.getLong(7);

                final JobDVResults event =
                        new JobDVResults(project_id, exec_id, job_id,
                                 check_time, result, result_value, expected_value);
                events.add(event);
            } while (rs.next());

            return events;
        }
    }

    private static final String SELECT_EXECUTOR_RESULTS =
            "SELECT exec_id, project_id,  job_id, check_time, result,result_value, expected_value FROM job_dv_results "
                    + " WHERE project_id=? ORDER BY check_time ";

    private static final String SELECT_EXECUTOR_RESULTS_FLOW =
            "SELECT exec_id, project_id,  job_id, check_time, result,result_value, expected_value FROM job_dv_results "
                    + " WHERE project_id=? and exec_id = ? ORDER BY check_time ";

    private static final String INSERT_EXECUTION_NODE = "INSERT INTO job_dv_results "
            + "(project_id,exec_id,  job_id,  check_time, "
            + "result, result_value, expected_value, job_status) VALUES (?,?,?,?,?,?,?,?)";
//1, 0, 1, shell_end, null, 1532377898291, PASS, 1

}
