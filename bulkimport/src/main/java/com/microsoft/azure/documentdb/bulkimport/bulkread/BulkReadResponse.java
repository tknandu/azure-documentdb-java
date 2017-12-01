/**
 * The MIT License (MIT)
 * Copyright (c) 2017 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.documentdb.bulkimport.bulkread;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class BulkReadResponse {
    /**
     * Total number of documents read.
     */
    final private int numberOfDocumentsRead;

    /**
     * Total request units consumed.
     */
    final private double totalRequestUnitsConsumed;

    /**
     * Total bulk import time.
     */
    final private Duration totalTimeTaken;

    /**
     * keeps failures which surfaced out
     */
    final private List<Exception> failures;
    
    /**
     * The documents read
     */
    final private List<Object> documentsRead;

    public BulkReadResponse(int numberOfDocumentsRead, double totalRequestUnitsConsumed, Duration totalTimeTaken, List<Exception> failures, List<Object> documentsRead) {
        this.numberOfDocumentsRead = numberOfDocumentsRead;
        this.totalRequestUnitsConsumed = totalRequestUnitsConsumed;
        this.totalTimeTaken = totalTimeTaken;
        this.failures = failures;
        this.documentsRead = documentsRead;
    }

    /**
     * Gets failure list if some documents failed to get read.
     *
     * @return list of errors or empty list if no error.
     */
    public List<Exception> getErrors() {
        return Collections.unmodifiableList(failures);
    }

    /**
     * Gets number of documents successfully read.
     *
     * <p> If this number is less than actual batch size (meaning some documents failed to get read),
     * use {@link #getErrors()} to get the failure cause.
     * @return the numberOfDocumentsRead
     */
    public int getNumberOfDocumentsRead() {
        return numberOfDocumentsRead;
    }

    /**
     * @return the totalRequestUnitsConsumed
     */
    public double getTotalRequestUnitsConsumed() {
        return totalRequestUnitsConsumed;
    }

    /**
     * Gets the documents successfully read.
     *
     */
    public List<Object> getDocumentsRead() {
    	return documentsRead;
    }
      
    /**
     * @return the totalTimeTaken
     */
    public Duration getTotalTimeTaken() {
        return totalTimeTaken;
    }
}
