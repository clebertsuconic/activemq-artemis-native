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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.activemq.artemis.nativo.jlibaio.ffm.AIORing.AIO_RING_HEADER_SIZE;

public class IOControl {
    private static final Logger logger = LoggerFactory.getLogger(IOControl.class);

    private static final long IOCB_SIZE = IOCBInit.IOCB_LAYOUT.byteSize();

    private final MemorySegment ptr;

    private final ReentrantLock iocbLock = new ReentrantLock();
    private final ReentrantLock pollLock = new ReentrantLock();

    private static final long OFFSET_IO_CONTEXT = 0;
    private static final long OFFSET_EVENTS = 8;
    private static final long OFFSET_IOCB_PTR = 16;
    private static final long OFFSET_QUEUE_SIZE = 24;
    private static final long OFFSET_IOCB_PUT = 28;
    private static final long OFFSET_IOCB_GET = 32;
    private static final long OFFSET_USED = 36;

    public static final MemoryLayout IO_CONTROL_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.ADDRESS.withName("ioContext"),
            ValueLayout.ADDRESS.withName("events"),
            ValueLayout.ADDRESS.withName("iocb"),
            ValueLayout.JAVA_INT.withName("queueSize"),
            ValueLayout.JAVA_INT.withName("iocbPut"),
            ValueLayout.JAVA_INT.withName("iocbGet"),
            ValueLayout.JAVA_INT.withName("used")
    ).withName("io_control");

    public IOControl(MemorySegment ptr) {
        if(ptr == null || ptr.address() == 0L) {
            throw new IllegalArgumentException("IOControl pointer cannot be null");
        }
        this.ptr = ptr;
    }

    public MemorySegment ioContext() {
        MemorySegment rawCtx = ptr.get(ValueLayout.ADDRESS, OFFSET_IO_CONTEXT);
        if(rawCtx == null || rawCtx.address() == 0) {
            return MemorySegment.NULL;
        }
        return rawCtx.reinterpret(AIO_RING_HEADER_SIZE);
    }

    public void setIoContext(MemorySegment ctx) {
        ptr.set(ValueLayout.ADDRESS, OFFSET_IO_CONTEXT, ctx);
    }

    public MemorySegment events() {
        return ptr.get(ValueLayout.ADDRESS, OFFSET_EVENTS);
    }

    public void setEvents(MemorySegment events) {
        ptr.set(ValueLayout.ADDRESS, OFFSET_EVENTS, events);
    }

    public int queueSize() {
        return ptr.get(ValueLayout.JAVA_INT, OFFSET_QUEUE_SIZE);
    }

    public void setQueueSize(int size) {
        ptr.set(ValueLayout.JAVA_INT, OFFSET_QUEUE_SIZE, size);
    }

    public int iocbPut() {
        return ptr.get(ValueLayout.JAVA_INT, OFFSET_IOCB_PUT);
    }

    private void setIocbPut(int value) {
        ptr.set(ValueLayout.JAVA_INT, OFFSET_IOCB_PUT, value);
    }

    public int iocbGet() {
        return ptr.get(ValueLayout.JAVA_INT, OFFSET_IOCB_GET);
    }

    private void setIocbGet(int value) {
        ptr.set(ValueLayout.JAVA_INT, OFFSET_IOCB_GET, value);
    }

    public int used() {
        return ptr.get(ValueLayout.JAVA_INT, OFFSET_USED);
    }

    private void setUsed(int value) {
        ptr.set(ValueLayout.JAVA_INT, OFFSET_USED, value);
    }

    public MemorySegment iocbArray() {
        return ptr.get(ValueLayout.ADDRESS, OFFSET_IOCB_PTR);
    }

    public MemorySegment iocbArrayWithSize() {
        MemorySegment array = iocbArray();
        if(array == null || array.address() == 0L) {
            throw new IllegalStateException("IOCB array NULL");
        }
        return array.reinterpret((long) queueSize() * ValueLayout.ADDRESS.byteSize());
    }

    public void setIocbArray(MemorySegment array) {
        ptr.set(ValueLayout.ADDRESS, OFFSET_IOCB_PTR, array);
    }

    public void withIocbLock(Runnable action) {
        iocbLock.lock();
        try {
            action.run();
        } finally {
            iocbLock.unlock();
        }
    }

    public void withPollLock(Runnable action) {
        pollLock.lock();
        try {
            action.run();
        } finally {
            pollLock.unlock();
        }
    }

    public MemorySegment getIOCB() {
        final MemorySegment[] iocb = new MemorySegment[1];
        withIocbLock(() -> {
            int queueSize = queueSize();
            int used = used();
            if(used >= queueSize) {
                return;
            }
            int getIdx = iocbGet();
            MemorySegment addrSeg = iocbArrayWithSize().getAtIndex(ValueLayout.ADDRESS, getIdx);

            if(addrSeg == null || addrSeg.address() == 0L) {
                logger.error("getIOCB: null IOCB at index {}", getIdx);
                iocb[0] = null;
                return;
            }

            setUsed(used + 1);
            setIocbGet((getIdx + 1) % queueSize);

            iocb[0] = MemorySegment.ofAddress(addrSeg.address()).reinterpret(IOCB_SIZE);
        });
        logger.trace("getIOCB:: used={}, queueSize={}, get={}, put={}",
                used(), queueSize(), iocbGet(), iocbPut());
        return iocb[0];
    }

    public void putIOCB(MemorySegment iocb) {
        if(iocb == null || iocb.address() == 0L) {
            logger.warn("putIOCB: null IOCB ignored");
            return;
        }
        withIocbLock(() -> {
            int queueSize = queueSize();
            // Add back to free slot at iocbPut
            int putIdx = iocbPut();
            if(putIdx < 0 || putIdx >= queueSize) {
                logger.error("putIOCB: invalid putIdx={} queueSize={}",
                        putIdx, queueSize);
                return;
            }
            setUsed(used() - 1);
            iocbArrayWithSize().setAtIndex(
                    ValueLayout.ADDRESS,
                    putIdx,
                    MemorySegment.ofAddress(iocb.address()));
            setIocbPut((putIdx + 1) % queueSize);
        });
        logger.trace("putIOCB:: used={}, queueSize={}, get={}, put={}",
                used(), queueSize(), iocbGet(), iocbPut());
    }

    public boolean isValid() {
        if(ioContext() == null || ioContext().address() == 0) {
            return false;
        }

        if(queueSize() <= 0) {
            return false;
        }

        int usedCount = used();
        if(usedCount < 0 || usedCount > queueSize()) {
            return false;
        }

        int iocbPutIdx = iocbPut(), iocbGetIdx = iocbGet(), queueSize = queueSize();
        return iocbPutIdx >= 0 && iocbPutIdx < queueSize &&
                iocbGetIdx >= 0 && iocbGetIdx < queueSize;
    }
}
