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
package org.apache.activemq.artemis.nativo.jlibaio.ffm.test;

import org.apache.activemq.artemis.nativo.jlibaio.ffm.IOControl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("IOControl Tests")
public class IOControlTest {
    private Arena testArena;
    private IOControl ioControl;
    private MemorySegment mockPtr;

    // Calculated offsets matching the private constants
    private static final long OFFSET_IO_CONTEXT = 0;
    private static final long OFFSET_EVENTS = 8;
    private static final long OFFSET_IOCB_PTR = 16;
    private static final long OFFSET_QUEUE_SIZE = 24;
    private static final long OFFSET_IOCB_PUT = 28;
    private static final long OFFSET_IOCB_GET = 32;
    private static final long OFFSET_USED = 36;
    private static final MemorySegment CTX = Arena.global().allocate(ValueLayout.ADDRESS);

    @BeforeEach
    void setUp(){
        testArena = Arena.ofConfined();
        long layoutSize = IOControl.IO_CONTROL_LAYOUT.byteSize();
        mockPtr = testArena.allocate(layoutSize, 1);

        mockPtr.set(ValueLayout.ADDRESS, OFFSET_IO_CONTEXT, CTX);
        mockPtr.set(ValueLayout.ADDRESS, OFFSET_EVENTS, MemorySegment.NULL);
        mockPtr.set(ValueLayout.ADDRESS, OFFSET_IOCB_PTR, MemorySegment.NULL);
        mockPtr.set(ValueLayout.JAVA_INT, OFFSET_QUEUE_SIZE, 10);
        mockPtr.set(ValueLayout.JAVA_INT, OFFSET_IOCB_PUT, 0);
        mockPtr.set(ValueLayout.JAVA_INT, OFFSET_IOCB_GET, 0);
        mockPtr.set(ValueLayout.JAVA_INT, OFFSET_USED, 0);

        ioControl = new IOControl(mockPtr);
    }

    @AfterEach
    void tearDown() {
        testArena.close();
    }

    @Nested
    @DisplayName("Constructor")
    class ConstructorTests{
        @Test
        public void constructorThrowsNullPtrExceptionTest() {
            assertThrows(IllegalArgumentException.class, () -> new IOControl(null));
        }

        @Test
        public void constructorThrowsNullSegmentExceptionTest() {
            assertThrows(IllegalArgumentException.class, () -> new IOControl(MemorySegment.NULL));
        }

        @Test
        public void constructorValidPtrTest() {
            assertDoesNotThrow(() -> new IOControl(mockPtr));
        }
    }

    @Nested
    @DisplayName("Getter and Setter Methods")
    class GetterSetterTest{

        @Test
        public void ioContextGetSetTest() {
            assertEquals(CTX, ioControl.ioContext());

            ioControl.setIoContext(CTX);
            assertEquals(CTX, ioControl.ioContext());
        }

        @Test
        public void eventGetSetTest() {
            assertEquals(MemorySegment.NULL, ioControl.events());

            MemorySegment events = testArena.allocate(8, 1);
            ioControl.setEvents(events);
            assertEquals(events, ioControl.events());
        }

        @Test
        public void queueSizetGetSetTest() {
            assertEquals(10, ioControl.queueSize());

            ioControl.setQueueSize(20);
            assertEquals(20, ioControl.queueSize());
        }

        @Test
        public void iocbPutGetTest() {
            assertEquals(0, ioControl.iocbPut());
        }

        @Test
        public void iocbGetGetTest() {
            assertEquals(0, ioControl.iocbGet());
        }

        @Test
        public void usedGetTest() {
            assertEquals(0, ioControl.used());
        }

        @Test
        public void iocbArrayGetSetTest() {
            assertEquals(MemorySegment.NULL, ioControl.iocbArray());
            MemorySegment array = testArena.allocate(8, 1);

            ioControl.setIocbArray(array);
            assertEquals(array, ioControl.iocbArray());
        }
    }

    @Nested
    @DisplayName("Lock Methods")
    class LockTest{

        @Test
        public void withIocbLockTest() {
            ioControl.withIocbLock(() -> {
                assertEquals(0, ioControl.iocbPut());
            });
        }

        @Test
        public void withPollLockTest() {
            ioControl.withPollLock(() -> {
                assertEquals(0, ioControl.iocbGet());
            });
        }
    }

    @Nested
    @DisplayName("getIOCB tests")
    class GetIOCBCoreTest {
        private MemorySegment iocbArray;

        @BeforeEach
        void setIocbArray() {
            iocbArray = testArena.allocate(80L, 1);
            ioControl.setIocbArray(iocbArray);

            // Verify pointer only
            assertFalse(ioControl.iocbArray().equals(MemorySegment.NULL));

            for(int i = 0; i < 10; i++) {
                iocbArray.setAtIndex(ValueLayout.ADDRESS, i, testArena.allocate(64L, 1));
            }

            ioControl.setQueueSize(10);
            setIocbPutDirect(0);
            setIocbGetDirect(0);
            setUsedDirect(0);
        }

        @Test
        public void getIOCBAvailableTest() {
            setUsedDirect(4);
            MemorySegment result = ioControl.getIOCB();

            assertNotNull(result);
            assertEquals(5, ioControl.used());
            assertEquals(1, ioControl.iocbGet());
        }

        @Test
        public void getIOCBNoAvailableTest() {
            // setup used == queueSize
            setUsedDirect(10);

            MemorySegment result = ioControl.getIOCB();

            assertNull(result);
        }

        @Test
        public void getIOCBWrapAroundGetIndexTest() {
            setIocbGetDirect(9);
            setUsedDirect(0);

            MemorySegment result = ioControl.getIOCB();

            assertNotNull(result);
            assertEquals(1, ioControl.used());
            assertEquals(0, ioControl.iocbGet());
        }

        @Test
        public void putIOCBTest(){
            MemorySegment iocbArray = testArena.allocate(80, 1);
            ioControl.setIocbArray(iocbArray);
            for(int i=0; i<10; i++) {
                iocbArray.setAtIndex(ValueLayout.ADDRESS, i, testArena.allocate(64L, 1));
            }
            ioControl.setQueueSize(10);

            setUsedDirect(1);
            MemorySegment iocb = ioControl.getIOCB();
            ioControl.putIOCB(iocb);
            assertEquals(1, ioControl.used());
            assertEquals(1, ioControl.iocbPut());
        }

        @Test
        public void putIOCBFullCycleTest(){
            setUsedDirect(1);
            MemorySegment iocb = ioControl.getIOCB();
            assertEquals(2, ioControl.used());

            ioControl.putIOCB(iocb);
            assertEquals(1, ioControl.used());
            assertEquals(1, ioControl.iocbPut());
        }

        @Test
        public void getIOCBQueueSizeZeroTest() {
            ioControl.setQueueSize(0);
            setUsedDirect(0);
            assertNull(ioControl.getIOCB());
        }

        @Test
        public void lockExceptionHandlingTest() {
            assertThrows(RuntimeException.class,() -> {
            ioControl.withIocbLock(() -> {
                throw new RuntimeException("test");
            });
        });
        }
    }

    @Nested
    @DisplayName("is valid tests")
    class IsValidTest {
        @Test
        public void isValidZeroContextTest() {
            ioControl.setIocbArray(testArena.allocate(80L, 1));
            ioControl.setEvents(testArena.allocate(8L, 1));
            mockPtr.set(ValueLayout.JAVA_LONG, OFFSET_IO_CONTEXT, 0L);
            assertFalse(ioControl.isValid());
        }

        @Test
        public void isValidZeroQueueSizeTest() {
            setupArrays();
            ioControl.setQueueSize(0);
            assertFalse(ioControl.isValid());
        }

        @Test
        public void isValidUsedExceedsQueueSizeTest() {
            setupArrays();
            setUsedDirect(11);
            assertFalse(ioControl.isValid());
        }

        @Test
        public void isValidNegativeUsedTest() {
            setupArrays();
            setUsedDirect(-1);
            assertFalse(ioControl.isValid());
        }

        @Test
        public void isValidInvalidPutIndex() {
            setupArrays();
            setIocbPutDirect(-1);
            assertFalse(ioControl.isValid());
        }
    }

    // Helper methods
    private void setUsedDirect(int value){
        mockPtr.set(ValueLayout.JAVA_INT, OFFSET_USED, value);
    }

    private int getUsedDirect(){
        return mockPtr.get(ValueLayout.JAVA_INT, OFFSET_USED);
    }

    private void setIocbPutDirect(int value){
        mockPtr.set(ValueLayout.JAVA_INT, OFFSET_IOCB_PUT, value);
    }

    private void setIocbGetDirect(int value){
        mockPtr.set(ValueLayout.JAVA_INT, OFFSET_IOCB_GET, value);
    }

    private void setupArrays() {
        ioControl.setIocbArray(testArena.allocate(80L, 1));
        ioControl.setEvents(testArena.allocate(8L, 1));
        ioControl.setQueueSize(10);
    }
}
