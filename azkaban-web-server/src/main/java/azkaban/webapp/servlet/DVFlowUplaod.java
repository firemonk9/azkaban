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

package azkaban.webapp.servlet;

import azkaban.Constants.ConfigurationKeys;
import azkaban.datavalidation.DataValidationManager;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.JobDVResults;
import azkaban.flow.Flow;
import azkaban.flowtrigger.quartz.FlowTriggerScheduler;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidatorConfigs;
import azkaban.scheduler.ScheduleManager;
import azkaban.server.session.Session;
import azkaban.user.Permission.Type;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserUtils;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanWebServer;
import com.dataq.azkaban.main.PrepareJob;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class DVFlowUplaod extends LoginAbstractAzkabanServlet {

    private static final String APPLICATION_JSON = "application/json";
    private static final long serialVersionUID = 1;
    private static final Logger logger = Logger
            .getLogger(DVFlowUplaod.class);

    private static final String LOCKDOWN_CREATE_PROJECTS_KEY =
            "lockdown.create.projects";
    private static final String LOCKDOWN_UPLOAD_PROJECTS_KEY =
            "lockdown.upload.projects";

    private static final String PROJECT_DOWNLOAD_BUFFER_SIZE_IN_BYTES =
            "project.download.buffer.size";
    private static final Comparator<Flow> FLOW_ID_COMPARATOR = new Comparator<Flow>() {
        @Override
        public int compare(final Flow f1, final Flow f2) {
            return f1.getId().compareTo(f2.getId());
        }
    };
    private ProjectManager projectManager;
    private ExecutorManagerAdapter executorManager;
    private ScheduleManager scheduleManager;
    private UserManager userManager;
    private FlowTriggerScheduler scheduler;
    private int downloadBufferSize;
    private boolean lockdownCreateProjects = false;
    private boolean lockdownUploadProjects = false;
    private boolean enableQuartz = false;
    private DataValidationManager dataValidationManager;

    @Override
    public void init(final ServletConfig config) throws ServletException {
        super.init(config);

        final AzkabanWebServer server = (AzkabanWebServer) getApplication();
        this.projectManager = server.getProjectManager();
        this.executorManager = server.getExecutorManager();
        this.scheduleManager = server.getScheduleManager();
        this.userManager = server.getUserManager();
        this.scheduler = server.getScheduler();
        this.lockdownCreateProjects =
                server.getServerProps().getBoolean(LOCKDOWN_CREATE_PROJECTS_KEY, false);
        this.enableQuartz = server.getServerProps().getBoolean(ConfigurationKeys.ENABLE_QUARTZ, false);

        this.dataValidationManager = server.getDataValidationManager();

        if (this.lockdownCreateProjects) {
            logger.info("Creation of projects is locked down");
        }

        this.lockdownUploadProjects =
                server.getServerProps().getBoolean(LOCKDOWN_UPLOAD_PROJECTS_KEY, false);
        if (this.lockdownUploadProjects) {
            logger.info("Uploading of projects is locked down");
        }

        this.downloadBufferSize =
                server.getServerProps().getInt(PROJECT_DOWNLOAD_BUFFER_SIZE_IN_BYTES,
                        8192);

        logger.info("downloadBufferSize: " + this.downloadBufferSize);
        logger.info("##################");
    }

    @Override
    protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
                             final Session session) throws ServletException, IOException {
        if (hasParam(req, "ajax")) {
            final Integer execId = hasParam(req, "exec_id") ? Integer.parseInt(getParam(req, "exec_id")) : null;

            List<JobDVResults> jobDVResultsList = dataValidationManager.fetchDVResults(execId);//updateDVResults(projectId, execId, jobId, pathToStoreResults,jobReturnStatus, resultCount, expectedCount);
            if (jobDVResultsList != null && jobDVResultsList.size() == 1) {
                JobDVResults res = jobDVResultsList.get(0);
                String zipFilePath = res.getPathToStoreResults();
                File f = new File(zipFilePath);
                byte[] bytes = GZIPUtils.unGzipBytes(Files.readAllBytes(Paths.get(zipFilePath)));
                final File tempDir = Utils.createTempDir();
                String fileName = f.getName().replace("zip", "json");
                Path path = Paths.get(tempDir.getAbsolutePath(), fileName);
                Files.write(path, bytes);
                Object obj = JSONUtils.parseJSONFromFile(new File(path.toAbsolutePath().toString()));
                this.writeJSON(resp, obj);
            }
//            final HashMap<String, Object> ret = new HashMap<>();
//            ajazHandleFetch(req, resp, ret, session);
        }
    }

//    @Override
//    protected void handleMultiformPost(final HttpServletRequest req,
//                                       final HttpServletResponse resp, final Map<String, Object> params, final Session session)
//            throws ServletException, IOException {
//        // Looks like a duplicate, but this is a move away from the regular
//        // multiform post + redirect
//        // to a more ajax like command.
//        final String action = (String) params.get("ajax");
//        final HashMap<String, String> ret = new HashMap<>();
//        if (action.equals("upload")) {
//            ajaxHandleUpload(req, resp, ret, params, session);
//        }
//
//        this.writeJSON(resp, ret);
//
//    }

    @Override
    protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
                              final Session session) throws ServletException, IOException {


        final User user = session.getUser();


        final Integer projectId = hasParam(req, "project_id") ? Integer.parseInt(getParam(req, "project_id")) : null;
        final String projectName = hasParam(req, "project_name") ? getParam(req, "project_name") : null;
        final String projectDescription = hasParam(req, "project_description") ? getParam(req, "project_description") : null;
        Project project = null;

        final HashMap<String, String> ret = new HashMap<>();
        try {

            InputStream is = req.getInputStream();
            ByteOutputStream fos = new ByteOutputStream();

            byte[] buf = new byte[1000];
            for (int nChunk = is.read(buf); nChunk != -1; nChunk = is.read(buf)) {
                fos.write(buf, 0, nChunk);
            }
            byte[] bytes = fos.getBytes();

            String status = null;
            String action = null;
            String message = null;
            HashMap<String, Object> params = null;

            if (projectId == null && projectName != null) {
                if (this.lockdownCreateProjects &&
                        !UserUtils.hasPermissionforAction(this.userManager, user, Type.CREATEPROJECTS)) {
                    message =
                            "User " + user.getUserId() + " doesn't have permission to create projects.";
                    logger.info(message);
                    status = "error";
                } else {
                    try {
                        project = this.projectManager.createProject(projectName, projectDescription, user);
                        status = "success";
                        action = "redirect";
                    } catch (final ProjectManagerException e) {
                        message = e.getMessage();
                        status = "error";
                    }
                }
                final String response = createJsonResponse(status, message, action, params);
                //Create Project with name

            } else if (projectId != null) {
                project = this.projectManager.getProject(projectId);
            } else {
                ret.put("ERROR", "both project_id and project_name cannot be null ");
                writeJSON(resp, ret);
            }

            String zipFilePath = PrepareJob.jsonToCommands(bytes) + ".zip";

            Map<String, ValidationReport> reports = this.projectManager.uploadProject(project, new File(zipFilePath), "zip", user, null);

            final StringBuffer errorMsgs = new StringBuffer();
            final StringBuffer warnMsgs = new StringBuffer();
            for (final Map.Entry<String, ValidationReport> reportEntry : reports.entrySet()) {
                final ValidationReport report = reportEntry.getValue();
                if (!report.getInfoMsgs().isEmpty()) {
                    for (final String msg : report.getInfoMsgs()) {
                        switch (ValidationReport.getInfoMsgLevel(msg)) {
                            case ERROR:
                                errorMsgs.append(ValidationReport.getInfoMsg(msg));
                                break;
                            case WARN:
                                warnMsgs.append(ValidationReport.getInfoMsg(msg));
                                break;
                            default:
                                break;
                        }
                    }
                }
                if (!report.getErrorMsgs().isEmpty()) {
                    errorMsgs.append("Validator " + reportEntry.getKey());
                    for (final String msg : report.getErrorMsgs()) {
                        errorMsgs.append(msg);
                    }
                }
                if (!report.getWarningMsgs().isEmpty()) {
                    warnMsgs.append("Validator " + reportEntry.getKey()
                            + " reports warnings");
                    for (final String msg : report.getWarningMsgs()) {
                        warnMsgs.append(msg);
                    }
                }
            }
            if (errorMsgs.length() > 0) {
                // If putting more than 4000 characters in the cookie, the entire
                // message
                // will somehow get discarded.
                registerError(ret, errorMsgs.length() > 4000 ? errorMsgs.substring(0, 4000)
                        : errorMsgs.toString(), resp, 500);
            }
            if (warnMsgs.length() > 0) {
                ret.put(
                        "warn",
                        warnMsgs.length() > 4000 ? warnMsgs.substring(0, 4000) : warnMsgs
                                .toString());
            }
            if (errorMsgs.length() == 0) {
                final ArrayList<Map<String, Object>> flowList =
                        new ArrayList<>();
                //Get project again to get flows after upload.
                project = this.projectManager.getProject(projectId);
                ret.put("project_id", projectId.toString());
                ret.put("project_name", project.getName());
                for (final Flow flow : project.getFlows()) {
                    if (!flow.isEmbeddedFlow()) {
                        final HashMap<String, Object> flowObj = new HashMap<>();
                        flowObj.put("flowId", flow.getId());
                        flowList.add(flowObj);
                        ret.put("flow_id", flow.getId());
                    }
                }
                if (flowList.size() > 1) {
                    logger.warn("There can be only one flow per project. " + project.getName() + " has more than one flow ");
                } else if (flowList.size() == 1) {
                    // execute the flow
                    writeJSON(resp, ret);

                }
            }
        } catch (final Exception e) {
            logger.info("Installation Failed.", e);
            String error = e.getMessage();
            if (error.length() > 512) {
                error =
                        error.substring(0, 512) + "<br>Too many errors to display.<br>";
            }
            registerError(ret, "Installation Failed.<br>" + error, resp, 500);
        } finally {
        }
        // writeJSON(resp, ret);

    }


    private void registerError(final Map<String, String> ret, final String error,
                               final HttpServletResponse resp, final int returnCode) {
        ret.put("error", error);
        resp.setStatus(returnCode);
    }

//    private void ajazHandleFetch(final HttpServletRequest req, final HttpServletResponse resp,
//                                 final Map<String, Object> ret, final Session session) {
//
//        final User user = session.getUser();
//
//        try {
//            final Integer projectId = getIntParam(req, "project_id");//(String) req.getParameter("projectId");
//            Integer exec_id = null;
//            if (hasParam(req, "exec_id")) {
//                exec_id = getIntParam(req, "exec_id");
//            }
//            java.util.List<JobDVResults> r = dataValidationManager.fetchDVResults(exec_id);
//            java.util.List list = new ArrayList<Map<String, Object>>();
//            for (JobDVResults e : r) {
//                Map<String, Object> m = e.getKeyValue();
//                list.add(m);
//            }
//            ret.put("dv_results", list);
//
//        } catch (ServletException e) {
//            e.printStackTrace();
//        }
//
//    }


    private void ajaxHandleUpload(final HttpServletRequest req, final HttpServletResponse resp,
                                  final Map<String, String> ret, final Map<String, Object> multipart, final Session session)
            throws ServletException, IOException {

        final User user = session.getUser();
        final Integer projectId = Integer.parseInt((String) multipart.get("project_id"));
        final String jobReturnStatus = (String) multipart.get("jobReturnStatus");
        final Long resultCount = Long.parseLong((String) multipart.get("resultCount"));
        final Long expectedCount = Long.parseLong((String) multipart.get("expectedCount"));
        final int execId = Integer.parseInt((String) multipart.get("exec_id"));
        final String jobId = (String) multipart.get("job_id");
        String flowId = "DEFAULT";
        //ExecutableNode node=null;
        try {
            ExecutableFlow flow = this.executorManager.getExecutableFlow(execId);
            //node = flow.getExecutableNodePath(jobId);
//            flowId = flow.getFlowId();

        } catch (ExecutorManagerException e) {
            e.printStackTrace();
        }

        final Project project = this.projectManager.getProject(projectId);

        logger.info(
                "Upload: reference of project " + projectId + " is " + System.identityHashCode(project));

        final String autoFix = (String) multipart.get("fix");

        final Props props = new Props();
        if (autoFix != null && autoFix.equals("off")) {
            props.put(ValidatorConfigs.CUSTOM_AUTO_FIX_FLAG_PARAM, "false");
        } else {
            props.put(ValidatorConfigs.CUSTOM_AUTO_FIX_FLAG_PARAM, "true");
        }

        ret.put("projectId", String.valueOf(project.getId()));

        final FileItem item = (FileItem) multipart.get("file");
        final String fileName = item.getName();
        String type = null;

        final String contentType = item.getContentType();
        if (contentType != null
                && (contentType.startsWith(APPLICATION_JSON)
                || contentType.startsWith("application/x-zip-compressed") || contentType
                .startsWith("application/octet-stream"))) {
            type = "zip";
        } else {
            item.delete();
            registerError(ret, "File type " + contentType + " unrecognized.", resp, 400);

            return;
        }

        final File tempDir = Utils.createTempDir();
        OutputStream out = null;
        try {
            logger.info("Uploading file  " + fileName);
            final File archiveFile = new File(tempDir, fileName);
            out = new BufferedOutputStream(new FileOutputStream(archiveFile));
            IOUtils.copy(item.getInputStream(), out);
            out.close();

            String pathToStoreResults = projectManager.getProps().getString("azkaban.dataValidation.resultsDir") + "/" + fileName;

            // move the file to new location with new name,

            FileUtils.moveFile(FileUtils.getFile(archiveFile.getAbsolutePath().toString()), FileUtils.getFile(pathToStoreResults));

            logger.info("writing to " + archiveFile.getAbsolutePath().toString());

            dataValidationManager.updateDVResults(projectId, execId, jobId, pathToStoreResults, jobReturnStatus, resultCount, expectedCount);

            //unscheduleall/scheduleall should only work with flow which has defined flow trigger
            //unschedule all flows within the old project


        } catch (final Exception e) {
            logger.info("Installation Failed.", e);
            String error = e.getMessage();
            if (error.length() > 512) {
                error =
                        error.substring(0, 512) + "<br>Too many errors to display.<br>";
            }
            registerError(ret, "Installation Failed.<br>" + error, resp, 500);
        } finally {
            if (out != null) {
                out.close();
            }
            if (tempDir.exists()) {
                FileUtils.deleteDirectory(tempDir);
            }
        }

        logger.info("Upload: project " + projectId + " version is " + project.getVersion()
                + ", reference is " + System.identityHashCode(project));
        ret.put("version", String.valueOf(project.getVersion()));
    }


    protected boolean hasPermission(final User user, final Type type) {
        for (final String roleName : user.getRoles()) {
            final Role role = this.userManager.getRole(roleName);
            if (role.getPermission().isPermissionSet(type)
                    || role.getPermission().isPermissionSet(Type.ADMIN)) {
                return true;
            }
        }

        return false;
    }


}
