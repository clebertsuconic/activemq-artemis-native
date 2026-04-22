/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.nativo.jlibaio.ffm;

import org.apache.activemq.artemis.nativo.jlibaio.SubmitInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CallbackRegistry<Callback extends SubmitInfo> {
//    private static final Logger logger = LoggerFactory.getLogger(CallbackRegistry.class);
    private final ConcurrentHashMap<Long, Callback> registry = new ConcurrentHashMap<>();
    private final AtomicLong idGenerator = new AtomicLong(0);

    /*
    * <p>
    * This method is thread-safe and lock-free. It avoids returning an ID of -1 and 0,
    * as -1 and 0 are reserved as a NULL/Invalid marker in the native libaio layer.
    *
    * @param callback the I/O completion handler to register.
    * @return a unique identifier (other than -1 or 0) for this callback.
    * */
    public long register(Callback callback) {
        Objects.requireNonNull(callback, "Callbacks cannot be null");
        long id;
        do {
            id = idGenerator.incrementAndGet();
        } while (id == -1 || id == 0);
        registry.put(id, callback);
//        logger.debug("CallbackRegistry::register id = {}, callback = {}", id, callback);
        return id;
    }

    public Callback findCallbackById(long id) {
        return registry.get(id);
    }

    public void removeCallbackById(long id) {
        this.registry.remove(id);
    }
}
