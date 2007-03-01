/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.jcr2spi.hierarchy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.jackrabbit.name.QName;

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * <code>ChildNodeAttic</code>...
 */
class ChildNodeAttic {

    private static Logger log = LoggerFactory.getLogger(ChildNodeAttic.class);

    private Set attic = new HashSet();

    ChildNodeAttic() {
    }

    boolean contains(QName name, int index) {
        for (Iterator it = attic.iterator(); it.hasNext();) {
            NodeEntryImpl ne = (NodeEntryImpl) it.next();
            if (ne.matches(name, index)) {
                return true;
            }
        }
        return false;
    }

    List get(QName name) {
        List l = new ArrayList();
        for (Iterator it = attic.iterator(); it.hasNext();) {
            NodeEntryImpl ne = (NodeEntryImpl) it.next();
            if (ne.matches(name)) {
                l.add(ne);
            }
        }
        return l;
    }

    /**
     *
     * @param name The original name of the NodeEntry before it has been moved.
     * @param index The original index of the NodeEntry before it has been moved.
     * @return
     */
    NodeEntry get(QName name, int index) {
        for (Iterator it = attic.iterator(); it.hasNext();) {
            NodeEntryImpl ne = (NodeEntryImpl) it.next();
            if (ne.matches(name, index)) {
                return ne;
            }
        }
        // not found
        return null;
    }

    /**
     *
     * @param uniqueId
     * @return
     */
    NodeEntry get(String uniqueId) {
        if (uniqueId == null) {
            throw new IllegalArgumentException();
        }
        for (Iterator it = attic.iterator(); it.hasNext();) {
            NodeEntryImpl ne = (NodeEntryImpl) it.next();
            if (uniqueId.equals(ne.getUniqueID())) {
                return ne;
            }
        }
        // not found
        return null;
    }

    void add(NodeEntryImpl movedEntry) {
        attic.add(movedEntry);
    }

    void remove(NodeEntryImpl movedEntry) {
        if (attic.contains(movedEntry)) {
            attic.remove(movedEntry);
        }
    }

    void clear() {
        if (attic != null) {
            attic.clear();
        }
    }
}