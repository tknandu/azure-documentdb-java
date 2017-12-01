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

import static com.microsoft.azure.documentdb.bulkimport.ExceptionUtils.isGone;
import static com.microsoft.azure.documentdb.bulkimport.ExceptionUtils.isSplit;
import static com.microsoft.azure.documentdb.bulkimport.ExceptionUtils.isThrottled;
import static com.microsoft.azure.documentdb.bulkimport.ExceptionUtils.isTimedOut;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ListenableFuture;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.StoredProcedureResponse;
import com.microsoft.azure.documentdb.bulkimport.BatchOperator;
import com.microsoft.azure.documentdb.bulkimport.InsertMetrics;
import com.microsoft.azure.documentdb.bulkimport.bulkread.BulkReadStoredProcedureResponse;

public class BatchReader {

	/**
	 *  The count of documents bulk read by this batch reader.
	 */
	public AtomicInteger numberOfDocumentsRead;
	
	/**
	 *  The documents bulk read by this batch reader.
	 */
	public List<Object> documentsRead;

	/**
	 * The total request units consumed by this batch reader.
	 */
	public AtomicDouble totalRequestUnitsConsumed;

	/**
	 * The link to the system bulk read stored procedure.
	 */
	private final String bulkreadSprocLink;

	/**
	 * The partition key property.
	 */
	private final String partitionKeyProperty;
	
	/**
	 * The group by property.
	 */
	private final String groupByProperty;

	/**
	 * The logger instance.
	 */
	private final Logger logger = LoggerFactory.getLogger(BatchReader.class);

	/**
	 * The index of the physical partition this batch operator is responsible for.
	 */
	protected String partitionKeyRangeId;

	/**
	 * The document client to use.
	 */
	protected DocumentClient client;

	/**
	 * Request options specifying the underlying partition key range id.
	 */
	protected RequestOptions requestOptions;

	protected static final ObjectMapper objectMapper = new ObjectMapper();

	public BatchReader(String partitionKeyRangeId, DocumentClient client, String bulkReadSprocLink, String partitionKeyProperty, String groupByProperty) {

		this.partitionKeyRangeId = partitionKeyRangeId;
		this.client = client;
		this.bulkreadSprocLink = bulkReadSprocLink;
		this.partitionKeyProperty = partitionKeyProperty;
		this.numberOfDocumentsRead = new AtomicInteger();
		this.totalRequestUnitsConsumed = new AtomicDouble();
		this.documentsRead = new ArrayList<Object>();
		this.groupByProperty = groupByProperty;
		
		class RequestOptionsInternal extends RequestOptions {
			RequestOptionsInternal(String partitionKeyRangeId) {
				setPartitionKeyRengeId(partitionKeyRangeId);
			}
		}

		this.requestOptions = new RequestOptionsInternal(partitionKeyRangeId);
		this.requestOptions.setScriptLoggingEnabled(true);
	}
	
	public List<Object> getdocumentsRead() {
		return documentsRead;
	}

	public int getNumberOfDocumentsRead() {
		return numberOfDocumentsRead.get();
	}

	public double getTotalRequestUnitsConsumed() {
		return totalRequestUnitsConsumed.get();
	}

	public BulkReadStoredProcedureResponse readAll() {
 		BulkReadStoredProcedureResponse returnedResponse = new BulkReadStoredProcedureResponse();
		try {
			logger.debug("pki {} Reading partition started", partitionKeyRangeId);
			Stopwatch stopwatch = Stopwatch.createStarted();
			double requestUnitsCounsumed = 0;
			int numberOfThrottles = 0;
			StoredProcedureResponse response;
			
            String lastContinuationPk = null;
            String lastContinuationToken = null;

            int lastErrorCode = -1;
			logger.debug("pki {} inside for loop, currentUpdateItemIndex", partitionKeyRangeId);

			boolean isThrottled = false;
			Duration retryAfter = Duration.ZERO;

			try {

				logger.debug("pki {}, Trying to read", partitionKeyRangeId);
            
				do {
					BulkReadStoredProcedureResponse bResponse = null;
					response = client.executeStoredProcedure(bulkreadSprocLink, requestOptions, new Object[] { groupByProperty, lastContinuationPk, lastContinuationToken, 5000, null });
	
					bResponse = parseFrom(response);
	
					if (bResponse != null) {
						if (bResponse.errorCode > 1) {
							logger.warn("pki {} Received response error code {}", partitionKeyRangeId, bResponse.errorCode);
						}	
						double requestCharge = response.getRequestCharge();
						requestUnitsCounsumed += requestCharge;
	                    lastContinuationPk = bResponse.continuationPk;
	                    lastContinuationToken = bResponse.continuationToken;
	                    lastErrorCode = bResponse.errorCode;
	                    returnedResponse.readResults.addAll(bResponse.readResults);
					}
					else {
						logger.warn("pki {} Failed to receive response", partitionKeyRangeId);
					}	
					
				}while(lastErrorCode != 1);
				
				returnedResponse.requestUnitsConsumed = requestUnitsCounsumed;
				numberOfDocumentsRead.addAndGet(returnedResponse.readResults.size());				
				
			} catch (DocumentClientException e) {

				logger.debug("pki {} Reading minibatch failed", partitionKeyRangeId, e);

				if (isThrottled(e)) {
					logger.debug("pki {} Throttled on partition range id", partitionKeyRangeId);
					numberOfThrottles++;
					isThrottled = true;
					retryAfter = Duration.ofMillis(e.getRetryAfterInMilliseconds());
					// will retry again

				} else if (isTimedOut(e)) {
					logger.debug("pki {} Request timed out", partitionKeyRangeId);
					boolean timedOut = true;
					// will retry again

				} else if (isGone(e)) {
					// there is no value in retrying
					if (isSplit(e)) {
						String errorMessage = String.format("pki %s is undergoing split, please retry shortly after re-initializing BulkImporter object", partitionKeyRangeId);
						logger.error(errorMessage);
						throw new RuntimeException(errorMessage);
					} else {
						String errorMessage = String.format("pki %s is gone, please retry shortly after re-initializing BulkImporter object", partitionKeyRangeId);
						logger.error(errorMessage);
						throw new RuntimeException(errorMessage);
					}

				} else {
					// there is no value in retrying
					String errorMessage = String.format("pki %s failed to read mini-batch. Exception was %s. Status code was %s",
							partitionKeyRangeId,
							e.getMessage(),
							e.getStatusCode());
					logger.error(errorMessage, e);
					throw new RuntimeException(e);
				}

				} catch (Exception e) {
					String errorMessage = String.format("pki %s Failed to read mini-batch. Exception was %s", partitionKeyRangeId,
							e.getMessage());
					logger.error(errorMessage, e);
					throw new RuntimeException(errorMessage, e);
				}

			 if (isThrottled) {
				try {
					logger.debug("pki {} throttled going to sleep for {} millis ", partitionKeyRangeId, retryAfter.toMillis());
					Thread.sleep(retryAfter.toMillis());
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			logger.debug("pki {} completed", partitionKeyRangeId);

			stopwatch.stop();
			return returnedResponse;
		} catch (Exception e) {
			throw e;
		}
	}

	private BulkReadStoredProcedureResponse parseFrom(StoredProcedureResponse storedProcResponse) throws JsonParseException, JsonMappingException, IOException {
		String res = storedProcResponse.getResponseAsString();
		logger.debug("Read for Partition Key Range Id {}: Stored Proc Response as String {}", partitionKeyRangeId, res);

		if (StringUtils.isEmpty(res))
			return null;

		return objectMapper.readValue(res, BulkReadStoredProcedureResponse.class);
	}
}
