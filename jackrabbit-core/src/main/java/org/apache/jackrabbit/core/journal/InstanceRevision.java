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
package org.apache.jackrabbit.core.journal;

/**
 * 
 */
public interface InstanceRevision {

    /**
     * Return current instance revision.
     *
     * @return instance revision
     * @throws JournalException if some error occurs
     */
    public long get() throws JournalException;

    /**
     * Set current instance revision.
     *
     * @param value new instance revision
     * @throws JournalException if some error occurs
     */
    public void set(long value) throws JournalException;
    
    /**
     * Closes the instance revision.
     */
    public void close();
}
