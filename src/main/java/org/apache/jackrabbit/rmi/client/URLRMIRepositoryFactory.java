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
package org.apache.jackrabbit.rmi.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.commons.repository.RepositoryFactory;
import org.apache.jackrabbit.rmi.remote.RemoteRepository;

/**
 * Factory that looks up a remote repository from a given URL.
 *
 * @since 1.4
 */
public class URLRMIRepositoryFactory implements RepositoryFactory {

    /**
     * Local adapter factory.
     */
    private final LocalAdapterFactory factory;

    /**
     * URL of the remote repository.
     */
    private final URL url;

    /**
     * Creates a factory for looking up a remote repository from a URL.
     *
     * @param factory local adapter factory
     * @param url URL or the remote repository
     */
    public URLRMIRepositoryFactory(LocalAdapterFactory factory, URL url) {
        this.factory = factory;
        this.url = url;
    }

    /**
     * Looks up and returns a remote repository from the configured URL.
     *
     * @return local adapter for the remote repository
     * @throws RepositoryException if the repository could not be accessed
     */
    public Repository getRepository() throws RepositoryException {
        try {
            ObjectInputStream input = new ObjectInputStream(url.openStream());
            try {
                Object remote = input.readObject();
                if (remote instanceof RemoteRepository) {
                    return factory.getRepository((RemoteRepository) remote);
                } else if (remote == null) {
                    throw new RepositoryException(
                            "Remote repository not found: The resource at "
                            + url + " is null");
                } else {
                    throw new RepositoryException(
                            "Invalid remote repository: The resource at "
                            + url + " is an instance of "
                            + remote.getClass().getName());
                }
            } finally {
                input.close();
            }
        } catch (ClassNotFoundException e) {
            throw new RepositoryException(
                    "Invalid remote repository: The resource at " + url
                    + " is an instance of an unknown class", e);
        } catch (IOException e) {
            throw new RepositoryException(
                    "Remote repository not found: The resource at " + url
                    + " could not be retrieved", e);
        }
    }

}
