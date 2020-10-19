/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.sqlparser.util;

import com.ads.api.beans.common.Node;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.MappingManagerUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.icc.util.RequestStatus;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Chirishma Kakarla / Dinesh Arasankala
 */
public class CreateMappingVersion {

    public static String preCreatingMapVersion(String json, int projectId, String mappingName, MappingManagerUtil mappingManagerUtil, int subjectId, KeyValueUtil keyValueUtil) {
        ObjectMapper mapper = new ObjectMapper();
        String resultStatus = null;
        json = json.replace(",\"childNodes\":[]", "");
        Mapping mapObj = null;
        try {
            mapObj = mapper.readValue(json, Mapping.class);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        try {
            ArrayList<MappingSpecificationRow> mappingSpecificationRowsList = (ArrayList<MappingSpecificationRow>) mapObj.getMappingSpecifications();

            resultStatus = creatingMapVersionForIncremental(projectId, mappingName, subjectId, mappingSpecificationRowsList, mappingManagerUtil, keyValueUtil);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return resultStatus;

    }

    public static String creatingMapVersionForIncremental(int projectId, String mappingName, int parentSubectId, ArrayList<MappingSpecificationRow> mapspecList, MappingManagerUtil mappingManagerUtil, KeyValueUtil keyValueUtil) {
        Mapping latestMappingObj = null;
        List<Float> latestMappingVersion = null;
        Float updateMappingVersion = 0.0f;
        RequestStatus resultStatus = null;
        int latestMapId = 0;
        try {
            latestMappingVersion = getMappingVersions(parentSubectId, mappingName, mappingManagerUtil, projectId);
            updateMappingVersion = latestMappingVersion.get(latestMappingVersion.size() - 1);
            Mapping mappingObj = null;
            try {

                if (parentSubectId > 0) {
                    mappingObj = mappingManagerUtil.getMapping(parentSubectId, Node.NodeType.MM_SUBJECT, mappingName, updateMappingVersion);
                } else {
                    mappingObj = mappingManagerUtil.getMapping(projectId, Node.NodeType.MM_PROJECT, mappingName, updateMappingVersion);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            int mappId = mappingObj.getMappingId();
            mappingObj.setProjectId(projectId);
            mappingObj.setSubjectId(parentSubectId);
            mappingObj.setMappingId(mappId);
            mappingObj.setChangedDescription("Mapping " + mappingName + " changed! as Version Done: " + updateMappingVersion);

            String status = mappingManagerUtil.versionMapping(mappingObj).getStatusMessage();

            List<Float> latestMapVersion = getMappingVersions(parentSubectId, mappingName, mappingManagerUtil, projectId);
            Float latestMapV = latestMapVersion.get(latestMapVersion.size() - 1);

            try {

                if (parentSubectId > 0) {
                    latestMappingObj = mappingManagerUtil.getMapping(parentSubectId, Node.NodeType.MM_SUBJECT, mappingName, latestMapV);
                } else {
                    latestMappingObj = mappingManagerUtil.getMapping(projectId, Node.NodeType.MM_PROJECT, mappingName, latestMapV);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            latestMapId = latestMappingObj.getMappingId();
            mappingManagerUtil.deleteMappingSpecifications(latestMapId);
            keyValueUtil.deleteKeyValues("8", latestMapId + "");

            resultStatus = mappingManagerUtil.addMappingSpecifications(latestMapId, mapspecList);
        } catch (Exception e) {
            StringWriter exceptionLog = new StringWriter();
            e.printStackTrace(new PrintWriter(exceptionLog));

        }
        return resultStatus.getStatusMessage() + "##" + latestMapId;
    }

    public static List<Float> getMappingVersions(int subjectId, String mapName, MappingManagerUtil mappingManagerUtil, int projectId) {
        List<Float> mapVersionList = new ArrayList();
        try {

            ArrayList<Mapping> mappings = null;

            try {

                if (subjectId > 0) {
                    mappings = mappingManagerUtil.getMappings(subjectId, Node.NodeType.MM_SUBJECT);
                } else {
                    mappings = mappingManagerUtil.getMappings(projectId, Node.NodeType.MM_PROJECT);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            if (!mappings.isEmpty()) {
                for (int map = 0; map < mappings.size(); map++) {
                    String mappingName = mappings.get(map).getMappingName();
                    float mappingVersion = mappings.get(map).getMappingSpecVersion();
                    if (mapName.equalsIgnoreCase(mappingName)) {
                        mapVersionList.add(mappingVersion);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
        return mapVersionList;
    }

}
