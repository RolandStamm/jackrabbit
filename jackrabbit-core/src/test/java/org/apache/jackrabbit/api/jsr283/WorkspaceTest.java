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
package org.apache.jackrabbit.api.jsr283;

import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

/**
 * <code>WorkspaceTest</code>...
 */
public class WorkspaceTest extends AbstractJCRTest {

    private Workspace workspace;

    protected void setUp() throws Exception {
        super.setUp();

        javax.jcr.Workspace wsp = superuser.getWorkspace();
        if (wsp instanceof Workspace) {
            workspace = (Workspace) wsp;
        } else {
            throw new NotExecutableException("JCR 2.0 Workspace expected.");
        }
    }

    /**
     * Tests {@link org.apache.jackrabbit.api.jsr283.Workspace#getLockManager()}.
     * 
     * @throws RepositoryException
     */
    public void testGetLockManager() throws RepositoryException {
        if (isSupported(Repository.OPTION_LOCKING_SUPPORTED)) {
            assertNotNull(workspace.getLockManager());
        } else {
            try {
                workspace.getLockManager();
                fail("UnsupportedRepositoryOperationException expected. Locking is not supported.");
            } catch (UnsupportedRepositoryOperationException e) {
                // success.
            }
        }
    }
}