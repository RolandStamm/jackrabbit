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
package org.apache.jackrabbit.jcr2spi.nodetype;

import org.apache.jackrabbit.spi.QItemDefinition;
import org.apache.jackrabbit.spi.QNodeDefinition;
import org.apache.jackrabbit.spi.QPropertyDefinition;
import org.apache.jackrabbit.spi.Name;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

/**
 * An <code>EffectiveNodeType</code> represents one or more
 * <code>NodeType</code>s as one 'effective' node type where inheritance
 * is resolved.
 * <p/>
 * Instances of <code>EffectiveNodeType</code> are immutable.
 */
public class EffectiveNodeTypeImpl implements Cloneable, EffectiveNodeType {
    private static Logger log = LoggerFactory.getLogger(EffectiveNodeTypeImpl.class);

    // list of explicitly aggregated {i.e. merged) node types
    private final TreeSet mergedNodeTypes = new TreeSet();
    // list of implicitly aggregated {through inheritance) node types
    private final TreeSet inheritedNodeTypes = new TreeSet();
    // list of all either explicitly (through aggregation) or implicitly
    // (through inheritance) included node types.
    private final TreeSet allNodeTypes = new TreeSet();
    // map of named item definitions (maps name to list of definitions)
    private final Map namedItemDefs = new HashMap();
    // list of unnamed item definitions (i.e. residual definitions)
    private final List unnamedItemDefs = new ArrayList();
    // (optional) set of additional mixins supported on node type
    private Set supportedMixins;

    /**
     * constructor.
     */
    EffectiveNodeTypeImpl(TreeSet mergedNodeTypes, TreeSet inheritedNodeTypes,
                          TreeSet allNodeTypes, Map namedItemDefs,
                          List unnamedItemDefs, Set supportedMixins) {
        this.mergedNodeTypes.addAll(mergedNodeTypes);
        this.inheritedNodeTypes.addAll(inheritedNodeTypes);
        this.allNodeTypes.addAll(allNodeTypes);
        Iterator iter = namedItemDefs.keySet().iterator();
        while (iter.hasNext()) {
            Object key = iter.next();
            List list = (List) namedItemDefs.get(key);
            this.namedItemDefs.put(key, new ArrayList(list));
        }
        this.unnamedItemDefs.addAll(unnamedItemDefs);

        if (supportedMixins != null) {
            this.supportedMixins = new HashSet();
            this.supportedMixins.addAll(supportedMixins);
        }
    }

    //--------------------------------------------------< EffectiveNodeType >---
    /**
     * @see EffectiveNodeType#getInheritedNodeTypes()
     */
    public Name[] getInheritedNodeTypes() {
        return (Name[]) inheritedNodeTypes.toArray(new Name[inheritedNodeTypes.size()]);
    }

    /**
     * @see EffectiveNodeType#getAllNodeTypes()
     */
    public Name[] getAllNodeTypes() {
        return (Name[]) allNodeTypes.toArray(new Name[allNodeTypes.size()]);
    }

    /**
     * @see EffectiveNodeType#getMergedNodeTypes()
     */
    public Name[] getMergedNodeTypes() {
        return (Name[]) mergedNodeTypes.toArray(new Name[mergedNodeTypes.size()]);
    }

    /**
     * @see EffectiveNodeType#getAllQNodeDefinitions()
     */
    public QNodeDefinition[] getAllQNodeDefinitions() {
        if (namedItemDefs.size() == 0 && unnamedItemDefs.size() == 0) {
            return QNodeDefinition.EMPTY_ARRAY;
        }
        ArrayList defs = new ArrayList(namedItemDefs.size() + unnamedItemDefs.size());
        Iterator iter = unnamedItemDefs.iterator();
        while (iter.hasNext()) {
            QItemDefinition qDef = (QItemDefinition) iter.next();
            if (qDef.definesNode()) {
                defs.add(qDef);
            }
        }
        iter = namedItemDefs.values().iterator();
        while (iter.hasNext()) {
            List list = (List) iter.next();
            Iterator iter1 = list.iterator();
            while (iter1.hasNext()) {
                QItemDefinition qDef = (QItemDefinition) iter1.next();
                if (qDef.definesNode()) {
                    defs.add(qDef);
                }
            }
        }
        if (defs.size() == 0) {
            return QNodeDefinition.EMPTY_ARRAY;
        }
        return (QNodeDefinition[]) defs.toArray(new QNodeDefinition[defs.size()]);
    }

    /**
     * @see EffectiveNodeType#getAllQPropertyDefinitions()
     */
    public QPropertyDefinition[] getAllQPropertyDefinitions() {
        if (namedItemDefs.size() == 0 && unnamedItemDefs.size() == 0) {
            return QPropertyDefinition.EMPTY_ARRAY;
        }
        ArrayList defs = new ArrayList(namedItemDefs.size() + unnamedItemDefs.size());
        Iterator iter = unnamedItemDefs.iterator();
        while (iter.hasNext()) {
            QItemDefinition qDef = (QItemDefinition) iter.next();
            if (!qDef.definesNode()) {
                defs.add(qDef);
            }
        }
        iter = namedItemDefs.values().iterator();
        while (iter.hasNext()) {
            List list = (List) iter.next();
            Iterator iter1 = list.iterator();
            while (iter1.hasNext()) {
                QItemDefinition qDef = (QItemDefinition) iter1.next();
                if (!qDef.definesNode()) {
                    defs.add(qDef);
                }
            }
        }
        if (defs.size() == 0) {
            return QPropertyDefinition.EMPTY_ARRAY;
        }
        return (QPropertyDefinition[]) defs.toArray(new QPropertyDefinition[defs.size()]);
    }

    /**
     * @see EffectiveNodeType#getAutoCreateQNodeDefinitions()
     */
    public QNodeDefinition[] getAutoCreateQNodeDefinitions() {
        // since auto-create items must have a name,
        // we're only searching the named item definitions
        if (namedItemDefs.size() == 0) {
            return QNodeDefinition.EMPTY_ARRAY;
        }
        ArrayList defs = new ArrayList(namedItemDefs.size());
        Iterator iter = namedItemDefs.values().iterator();
        while (iter.hasNext()) {
            List list = (List) iter.next();
            Iterator iter1 = list.iterator();
            while (iter1.hasNext()) {
                QItemDefinition qDef = (QItemDefinition) iter1.next();
                if (qDef.definesNode() && qDef.isAutoCreated()) {
                    defs.add(qDef);
                }
            }
        }
        if (defs.size() == 0) {
            return QNodeDefinition.EMPTY_ARRAY;
        }
        return (QNodeDefinition[]) defs.toArray(new QNodeDefinition[defs.size()]);
    }

    /**
     * @see EffectiveNodeType#getAutoCreateQPropertyDefinitions()
     */
    public QPropertyDefinition[] getAutoCreateQPropertyDefinitions() {
        // since auto-create items must have a name,
        // we're only searching the named item definitions
        if (namedItemDefs.size() == 0) {
            return QPropertyDefinition.EMPTY_ARRAY;
        }
        ArrayList defs = new ArrayList(namedItemDefs.size());
        Iterator iter = namedItemDefs.values().iterator();
        while (iter.hasNext()) {
            List list = (List) iter.next();
            Iterator iter1 = list.iterator();
            while (iter1.hasNext()) {
                QItemDefinition qDef = (QItemDefinition) iter1.next();
                if (!qDef.definesNode() && qDef.isAutoCreated()) {
                    defs.add(qDef);
                }
            }
        }
        if (defs.size() == 0) {
            return QPropertyDefinition.EMPTY_ARRAY;
        }
        return (QPropertyDefinition[]) defs.toArray(new QPropertyDefinition[defs.size()]);
    }

    /**
     * @see EffectiveNodeType#getMandatoryQPropertyDefinitions()
     */
    public QPropertyDefinition[] getMandatoryQPropertyDefinitions() {
        // since mandatory items must have a name,
        // we're only searching the named item definitions
        if (namedItemDefs.size() == 0) {
            return QPropertyDefinition.EMPTY_ARRAY;
        }
        ArrayList defs = new ArrayList(namedItemDefs.size());
        Iterator iter = namedItemDefs.values().iterator();
        while (iter.hasNext()) {
            List list = (List) iter.next();
            Iterator iter1 = list.iterator();
            while (iter1.hasNext()) {
                QItemDefinition qDef = (QItemDefinition) iter1.next();
                if (!qDef.definesNode() && qDef.isMandatory()) {
                    defs.add(qDef);
                }
            }
        }
        if (defs.size() == 0) {
            return QPropertyDefinition.EMPTY_ARRAY;
        }
        return (QPropertyDefinition[]) defs.toArray(new QPropertyDefinition[defs.size()]);
    }

    /**
     * @see EffectiveNodeType#getMandatoryQNodeDefinitions()
     */
    public QNodeDefinition[] getMandatoryQNodeDefinitions() {
        // since mandatory items must have a name,
        // we're only searching the named item definitions
        if (namedItemDefs.size() == 0) {
            return QNodeDefinition.EMPTY_ARRAY;
        }
        ArrayList defs = new ArrayList(namedItemDefs.size());
        Iterator iter = namedItemDefs.values().iterator();
        while (iter.hasNext()) {
            List list = (List) iter.next();
            Iterator iter1 = list.iterator();
            while (iter1.hasNext()) {
                QItemDefinition qDef = (QItemDefinition) iter1.next();
                if (qDef.definesNode() && qDef.isMandatory()) {
                    defs.add(qDef);
                }
            }
        }
        if (defs.size() == 0) {
            return QNodeDefinition.EMPTY_ARRAY;
        }
        return (QNodeDefinition[]) defs.toArray(new QNodeDefinition[defs.size()]);
    }

    /**
     * @see EffectiveNodeType#getNamedQNodeDefinitions(Name)
     */
    public QNodeDefinition[] getNamedQNodeDefinitions(Name name) {
        List list = (List) namedItemDefs.get(name);
        if (list == null || list.size() == 0) {
            return QNodeDefinition.EMPTY_ARRAY;
        }
        ArrayList defs = new ArrayList(list.size());
        Iterator iter = list.iterator();
        while (iter.hasNext()) {
            QItemDefinition qDef = (QItemDefinition) iter.next();
            if (qDef.definesNode()) {
                defs.add(qDef);
            }
        }
        if (defs.size() == 0) {
            return QNodeDefinition.EMPTY_ARRAY;
        }
        return (QNodeDefinition[]) defs.toArray(new QNodeDefinition[defs.size()]);
    }

    /**
     * @see EffectiveNodeType#getUnnamedQNodeDefinitions()
     */
    public QNodeDefinition[] getUnnamedQNodeDefinitions() {
        if (unnamedItemDefs.size() == 0) {
            return QNodeDefinition.EMPTY_ARRAY;
        }
        ArrayList defs = new ArrayList(unnamedItemDefs.size());
        Iterator iter = unnamedItemDefs.iterator();
        while (iter.hasNext()) {
            QItemDefinition qDef = (QItemDefinition) iter.next();
            if (qDef.definesNode()) {
                defs.add(qDef);
            }
        }
        if (defs.size() == 0) {
            return QNodeDefinition.EMPTY_ARRAY;
        }
        return (QNodeDefinition[]) defs.toArray(new QNodeDefinition[defs.size()]);
    }

    /**
     * @see EffectiveNodeType#getNamedQPropertyDefinitions(Name)
     */
    public QPropertyDefinition[] getNamedQPropertyDefinitions(Name name) {
        List list = (List) namedItemDefs.get(name);
        if (list == null || list.size() == 0) {
            return QPropertyDefinition.EMPTY_ARRAY;
        }
        ArrayList defs = new ArrayList(list.size());
        Iterator iter = list.iterator();
        while (iter.hasNext()) {
            QItemDefinition qDef = (QItemDefinition) iter.next();
            if (!qDef.definesNode()) {
                defs.add(qDef);
            }
        }
        if (defs.size() == 0) {
            return QPropertyDefinition.EMPTY_ARRAY;
        }
        return (QPropertyDefinition[]) defs.toArray(new QPropertyDefinition[defs.size()]);
    }

    /**
     * @see EffectiveNodeType#getUnnamedQPropertyDefinitions()
     */
    public QPropertyDefinition[] getUnnamedQPropertyDefinitions() {
        if (unnamedItemDefs.size() == 0) {
            return QPropertyDefinition.EMPTY_ARRAY;
        }
        ArrayList defs = new ArrayList(unnamedItemDefs.size());
        Iterator iter = unnamedItemDefs.iterator();
        while (iter.hasNext()) {
            QItemDefinition qDef = (QItemDefinition) iter.next();
            if (!qDef.definesNode()) {
                defs.add(qDef);
            }
        }
        if (defs.size() == 0) {
            return QPropertyDefinition.EMPTY_ARRAY;
        }
        return (QPropertyDefinition[]) defs.toArray(new QPropertyDefinition[defs.size()]);
    }

    /**
     * @inheritDoc
     */
    public boolean includesNodeType(Name nodeTypeName) {
        return allNodeTypes.contains(nodeTypeName);
    }

    /**
     * @inheritDoc
     */
    public boolean includesNodeTypes(Name[] nodeTypeNames) {
        return allNodeTypes.containsAll(Arrays.asList(nodeTypeNames));
    }

    /**
     * @inheritDoc
     * @see EffectiveNodeType#supportsMixin(Name)
     */
    public boolean supportsMixin(Name mixin) {
        if (supportedMixins == null) {
            return true;
        }
        else {
            return supportedMixins.contains(mixin);
        }
    }

    /**
     * @inheritDoc
     * @see EffectiveNodeType#checkAddNodeConstraints(Name, ItemDefinitionProvider)
     */
    public void checkAddNodeConstraints(Name name, ItemDefinitionProvider definitionProvider)
            throws ConstraintViolationException {
        try {
            definitionProvider.getQNodeDefinition(this, name, null);
        } catch (NoSuchNodeTypeException e) {
            String msg = "internal eror: inconsistent node type";
            log.debug(msg);
            throw new ConstraintViolationException(msg, e);
        }
    }

    /**
     * @inheritDoc
     * @see EffectiveNodeType#checkAddNodeConstraints(Name, ItemDefinitionProvider)
     */
    public void checkAddNodeConstraints(Name name, Name nodeTypeName, ItemDefinitionProvider definitionProvider)
            throws ConstraintViolationException, NoSuchNodeTypeException {
        QNodeDefinition nd = definitionProvider.getQNodeDefinition(this, name, nodeTypeName);
        if (nd.isProtected()) {
            throw new ConstraintViolationException(name + " is protected");
        }
        if (nd.isAutoCreated()) {
            throw new ConstraintViolationException(name + " is auto-created and can not be manually added");
        }
    }

    /**
     * @inheritDoc
     * @see EffectiveNodeType#checkRemoveItemConstraints(Name)
     */
    public void checkRemoveItemConstraints(Name name) throws ConstraintViolationException {
        /**
         * as there might be multiple definitions with the same name and we
         * don't know which one is applicable, we check all of them
         */
        QItemDefinition[] defs = getNamedItemDefs(name);
        if (defs != null) {
            for (int i = 0; i < defs.length; i++) {
                if (defs[i].isMandatory()) {
                    throw new ConstraintViolationException("can't remove mandatory item");
                }
                if (defs[i].isProtected()) {
                    throw new ConstraintViolationException("can't remove protected item");
                }
            }
        }
    }

    //---------------------------------------------< impl. specific methods >---

    private QItemDefinition[] getNamedItemDefs() {
        if (namedItemDefs.size() == 0) {
            return QItemDefinition.EMPTY_ARRAY;
        }
        ArrayList defs = new ArrayList(namedItemDefs.size());
        Iterator iter = namedItemDefs.values().iterator();
        while (iter.hasNext()) {
            defs.addAll((List) iter.next());
        }
        if (defs.size() == 0) {
            return QItemDefinition.EMPTY_ARRAY;
        }
        return (QItemDefinition[]) defs.toArray(new QItemDefinition[defs.size()]);
    }

    private QItemDefinition[] getNamedItemDefs(Name name) {
        List list = (List) namedItemDefs.get(name);
        if (list == null || list.size() == 0) {
            return QNodeDefinition.EMPTY_ARRAY;
        }
        return (QItemDefinition[]) list.toArray(new QItemDefinition[list.size()]);
    }

    private QItemDefinition[] getUnnamedItemDefs() {
        if (unnamedItemDefs.size() == 0) {
            return QItemDefinition.EMPTY_ARRAY;
        }
        return (QItemDefinition[]) unnamedItemDefs.toArray(new QItemDefinition[unnamedItemDefs.size()]);
    }

    /**
     * Merges another <code>EffectiveNodeType</code> with this one.
     * Checks for merge conflicts.
     *
     * @param other
     * @return
     * @throws ConstraintViolationException
     */
    EffectiveNodeTypeImpl merge(EffectiveNodeTypeImpl other)
            throws ConstraintViolationException {
        // create a clone of this instance and perform the merge on
        // the 'clone' to avoid a potentially inconsistant state
        // of this instance if an exception is thrown during
        // the merge.
        EffectiveNodeTypeImpl copy = (EffectiveNodeTypeImpl) clone();
        copy.internalMerge(other, false);
        return copy;
    }

    /**
     * Internal helper method which merges another <code>EffectiveNodeType</code>
     * instance with <i>this</i> instance.
     * <p/>
     * Warning: This instance might be in an inconsistent state if an exception
     * is thrown.
     *
     * @param other
     * @param supertype true if the merge is a result of inheritance, i.e. <code>other</code>
     *                  represents one or more supertypes of this instance; otherwise false, i.e.
     *                  the merge is the result of an explicit aggregation
     * @throws ConstraintViolationException
     */
    synchronized void internalMerge(EffectiveNodeTypeImpl other, boolean supertype)
            throws ConstraintViolationException {
        Name[] nta = other.getAllNodeTypes();
        int includedCount = 0;
        for (int i = 0; i < nta.length; i++) {
            if (includesNodeType(nta[i])) {
                // redundant node type
                log.debug("node type '" + nta[i] + "' is already contained.");
                includedCount++;
            }
        }
        if (includedCount == nta.length) {
            // total overlap, ignore
            return;
        }

        // named item definitions
        QItemDefinition[] defs = other.getNamedItemDefs();
        for (int i = 0; i < defs.length; i++) {
            QItemDefinition qDef = defs[i];
            if (includesNodeType(qDef.getDeclaringNodeType())) {
                // ignore redundant definitions
                continue;
            }
            Name name = qDef.getName();
            List existingDefs = (List) namedItemDefs.get(name);
            if (existingDefs != null) {
                if (existingDefs.size() > 0) {
                    // there already exists at least one definition with that name
                    for (int j = 0; j < existingDefs.size(); j++) {
                        QItemDefinition qItemDef = (QItemDefinition) existingDefs.get(j);
                        // make sure none of them is auto-create
                        if (qDef.isAutoCreated() || qItemDef.isAutoCreated()) {
                            // conflict
                            String msg = "The item definition for '" + name
                                    + "' in node type '"
                                    + qDef.getDeclaringNodeType()
                                    + "' conflicts with the one of node type '"
                                    + qItemDef.getDeclaringNodeType()
                                    + "': name collision with auto-create definition";
                            log.debug(msg);
                            throw new ConstraintViolationException(msg);
                        }
                        // check ambiguous definitions
                        if (qDef.definesNode() == qItemDef.definesNode()) {
                            if (!qDef.definesNode()) {
                                // property definition
                                QPropertyDefinition pd = (QPropertyDefinition) qDef;
                                QPropertyDefinition epd = (QPropertyDefinition) qItemDef;
                                // compare type & multiValued flag
                                if (pd.getRequiredType() == epd.getRequiredType()
                                        && pd.isMultiple() == epd.isMultiple()) {
                                    // conflict
                                    String msg = "The property definition for '"
                                            + name + "' in node type '"
                                            + qDef.getDeclaringNodeType()
                                            + "' conflicts with the one of node type '"
                                            + qItemDef.getDeclaringNodeType()
                                            + "': ambiguous property definition. "
                                            + "they must differ in required type "
                                            + "or cardinality.";
                                    log.debug(msg);
                                    throw new ConstraintViolationException(msg);
                                }
                            } else {
                                // child node definition
                                // conflict
                                String msg = "The child node definition for '"
                                        + name + "' in node type '"
                                        + qDef.getDeclaringNodeType()
                                        + "' conflicts with the one of node type '"
                                        + qItemDef.getDeclaringNodeType()
                                        + "': ambiguous child node definition. name must differ.";
                                log.debug(msg);
                                throw new ConstraintViolationException(msg);
                            }
                        }
                    }
                }
            } else {
                existingDefs = new ArrayList();
                namedItemDefs.put(name, existingDefs);
            }
            existingDefs.add(qDef);
        }

        // residual item definitions
        defs = other.getUnnamedItemDefs();
        for (int i = 0; i < defs.length; i++) {
            QItemDefinition qDef = defs[i];
            if (includesNodeType(qDef.getDeclaringNodeType())) {
                // ignore redundant definitions
                continue;
            }
            Iterator iter = unnamedItemDefs.iterator();
            while (iter.hasNext()) {
                QItemDefinition existing = (QItemDefinition) iter.next();
                // compare with existing definition
                if (qDef.definesNode() == existing.definesNode()) {
                    if (!qDef.definesNode()) {
                        // property definition
                        QPropertyDefinition pd = (QPropertyDefinition) qDef;
                        QPropertyDefinition epd = (QPropertyDefinition) existing;
                        // compare type & multiValued flag
                        if (pd.getRequiredType() == epd.getRequiredType()
                                && pd.isMultiple() == epd.isMultiple()
                                && pd.getOnParentVersion() == epd.getOnParentVersion()) {
                            // conflict
                            // TODO: need to take more aspects into account
                            // TODO: getMatchingPropDef needs to check this as well
                            String msg = "A property definition in node type '"
                                    + qDef.getDeclaringNodeType()
                                    + "' conflicts with node type '"
                                    + existing.getDeclaringNodeType()
                                    + "': ambiguous residual property definition";
                            log.debug(msg);
                            throw new ConstraintViolationException(msg);
                        }
                    } else {
                        // child node definition
                        QNodeDefinition nd = (QNodeDefinition) qDef;
                        QNodeDefinition end = (QNodeDefinition) existing;
                        // compare required & default primary types
                        if (Arrays.equals(nd.getRequiredPrimaryTypes(), end.getRequiredPrimaryTypes())
                                && (nd.getDefaultPrimaryType() == null
                                ? end.getDefaultPrimaryType() == null
                                : nd.getDefaultPrimaryType().equals(end.getDefaultPrimaryType()))) {
                            // conflict
                            String msg = "A child node definition in node type '"
                                    + qDef.getDeclaringNodeType()
                                    + "' conflicts with node type '"
                                    + existing.getDeclaringNodeType()
                                    + "': ambiguous residual child node definition";
                            log.debug(msg);
                            throw new ConstraintViolationException(msg);
                        }
                    }
                }
            }
            unnamedItemDefs.add(qDef);
        }
        for (int i = 0; i < nta.length; i++) {
            allNodeTypes.add(nta[i]);
        }

        if (supertype) {
            // implicit merge as result of inheritance

            // add other merged node types as supertypes
            nta = other.getMergedNodeTypes();
            for (int i = 0; i < nta.length; i++) {
                inheritedNodeTypes.add(nta[i]);
            }
            // add supertypes of other merged node types as supertypes
            nta = other.getInheritedNodeTypes();
            for (int i = 0; i < nta.length; i++) {
                inheritedNodeTypes.add(nta[i]);
            }
        } else {
            // explicit merge

            // merge with other merged node types
            nta = other.getMergedNodeTypes();
            for (int i = 0; i < nta.length; i++) {
                mergedNodeTypes.add(nta[i]);
            }
            // add supertypes of other merged node types as supertypes
            nta = other.getInheritedNodeTypes();
            for (int i = 0; i < nta.length; i++) {
                inheritedNodeTypes.add(nta[i]);
            }
        }
    }

    protected Object clone() {
        EffectiveNodeTypeImpl clone = new EffectiveNodeTypeImpl(mergedNodeTypes,
                inheritedNodeTypes, allNodeTypes, namedItemDefs, unnamedItemDefs,
                supportedMixins);
        return clone;
    }
}
