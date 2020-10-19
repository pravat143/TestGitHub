/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.metadata;

import com.ads.api.beans.mm.Subject;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.util.SystemManagerUtil;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author InkolluReddy
 */
public class Parentid {

    static MappingManagerUtil mutil = new MappingManagerUtil("AUTH_TOKEN");

    LinkedList subjectlist = new LinkedList();

    public List getSubjectId(Subject subject, MappingManagerUtil mappingManagerUtil) {

        try {

            int parentSubjectId = subject.getParentSubjectId();
            subjectlist.add(subject.getSubjectName());
            if (parentSubjectId != -1) {

                getSubjectId(mappingManagerUtil.getSubject(parentSubjectId), mappingManagerUtil);

            }

            System.out.println(subjectlist);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return subjectlist;

    }
    //  public static void main(String[] args) {

    //    getSubjectId(new Subject(),mutil);
    //}
}
