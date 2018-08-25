/*
 * Copyright 2012 LinkedIn Corp.
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

import org.apache.commons.collections.map.HashedMap;

import java.util.Map;

/**
 * Class to represent events on Azkaban executors
 *
 * @author gaggarwa
 */
public class JobDVResults {

    private final int projectId;
    private final int execid;
    private final String jobId;
    private final String jobReturnStatus;
    private final Long resultCount;
    private final Long expectedCount;
    private final Long check_time;
    private final String pathToStoreResults;

    public String getPathToStoreResults() {
        return pathToStoreResults;
    }

    public Long getCheck_time() {
        return check_time;
    }

    public int getProjectId() {
        return projectId;
    }

    public int getExecid() {
        return execid;
    }

    public String getJobId() {
        return jobId;
    }


    public String getJobReturnStatus() {
        return jobReturnStatus;
    }

    public Long getResultCount() {
        return resultCount;
    }

    public Long getExpectedCount() {
        return expectedCount;
    }


    public Map<String, Object> getKeyValue() {
        Map<String, Object> e = new HashedMap();
        e.put("execid", getExecid());
        e.put("projectId", getProjectId());
        e.put("jobId", getJobId());
        e.put("jobReturnStatus", getJobReturnStatus());
        e.put("resultCount", getResultCount());
        e.put("expectedCount", getExpectedCount());
        e.put("check_time", getCheck_time());
        e.put("pathToStoreResults",getPathToStoreResults());
        return e;

    }


    public JobDVResults(int projectId, int execId, String jobId, String pathToStoreResults,Long check_time, String jobReturnStatus, Long resultCount, Long expectedCount) {

        this.projectId = projectId;
        this.execid = execId;
        this.jobId = jobId;
        this.jobReturnStatus = jobReturnStatus;
        this.resultCount = resultCount;
        this.expectedCount = expectedCount;
        this.check_time = check_time;
        this.pathToStoreResults = pathToStoreResults;
    }
}
