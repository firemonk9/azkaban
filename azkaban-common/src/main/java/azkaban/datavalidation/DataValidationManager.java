package azkaban.datavalidation;

import azkaban.executor.ExecutorManagerException;
import azkaban.executor.JobDVResults;
import azkaban.executor.JobDVResultsDao;
import org.apache.log4j.Logger;

import javax.inject.Inject;
import java.util.List;

public class DataValidationManager {

    private static final Logger logger = Logger.getLogger(DataValidationManager.class);
    final JobDVResultsDao jobDVResultsDao;


    /**
     * Give the schedule manager a loader class that will properly load the schedule.
     */
    @Inject
    public DataValidationManager(final JobDVResultsDao jobDVResultsDao) {
        this.jobDVResultsDao = jobDVResultsDao;
    }


    public void updateDVResults(int projectId, int execid, String jobId, String jobReturnStatus, Long resultCount, Long expectedCount) {

        try {
            jobDVResultsDao.insertJobResults(projectId, execid, jobId, resultCount, expectedCount,jobReturnStatus);
        } catch (ExecutorManagerException e) {
            e.printStackTrace();
        }
    }

    public java.util.List<JobDVResults> fetchDVResults(Integer projectId, Integer exec_id) {
        List<JobDVResults> re = null;
        try {
            re=jobDVResultsDao.fetchJobResults(projectId, exec_id);

        } catch (ExecutorManagerException e) {
            e.printStackTrace();
        }
        return re;
    }

}
