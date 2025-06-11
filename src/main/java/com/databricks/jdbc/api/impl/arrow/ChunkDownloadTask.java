package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportLatencyLog;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/** Task class to manage download for a single chunk. */
class ChunkDownloadTask implements DatabricksCallableTask {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ChunkDownloadTask.class);
  public static final int MAX_RETRIES = 5;
  private static final long RETRY_DELAY_MS = 1500; // 1.5 seconds
  private final ArrowResultChunk chunk;
  private final IDatabricksHttpClient httpClient;
  private final ChunkDownloadManager chunkDownloader;
  private final IDatabricksConnectionContext connectionContext;
  private final String statementId;
  private final ChunkLinkDownloadService<ArrowResultChunk> linkDownloadService;
  Throwable uncaughtException = null;

  ChunkDownloadTask(
      ArrowResultChunk chunk,
      IDatabricksHttpClient httpClient,
      ChunkDownloadManager chunkDownloader,
      ChunkLinkDownloadService<ArrowResultChunk> linkDownloadService) {
    this.chunk = chunk;
    this.httpClient = httpClient;
    this.chunkDownloader = chunkDownloader;
    this.connectionContext = DatabricksThreadContextHolder.getConnectionContext();
    this.statementId = DatabricksThreadContextHolder.getStatementId();
    this.linkDownloadService = linkDownloadService;
  }

  @Override
  public Void call() throws DatabricksSQLException, ExecutionException, InterruptedException {
    long startTime = System.currentTimeMillis();
    int retries = 0;
    boolean downloadSuccessful = false;

    // Sets context in the newly spawned thread
    DatabricksThreadContextHolder.setChunkId(chunk.getChunkIndex());
    DatabricksThreadContextHolder.setConnectionContext(this.connectionContext);
    DatabricksThreadContextHolder.setStatementId(this.statementId);

    try {
      DatabricksThreadContextHolder.setRetryCount(retries);
      while (!downloadSuccessful) {
        try {
          if (chunk.isChunkLinkInvalid()) {
            ExternalLink link =
                linkDownloadService
                    .getLinkForChunk(chunk.getChunkIndex())
                    .get(); // Block until link is available
            chunk.setChunkLink(link);
          }

          chunk.downloadData(httpClient, chunkDownloader.getCompressionCodec());
          downloadSuccessful = true;
        } catch (DatabricksParsingException | IOException e) {
          retries++;
          if (retries >= MAX_RETRIES) {
            LOGGER.error(
                e,
                "Failed to download chunk after %d attempts. Chunk index: %d, Error: %s",
                MAX_RETRIES,
                chunk.getChunkIndex(),
                e.getMessage());
            chunk.setStatus(ChunkStatus.DOWNLOAD_FAILED);
            throw new DatabricksSQLException(
                "Failed to download chunk after multiple attempts",
                e,
                DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR);
          } else {
            LOGGER.warn(
                String.format(
                    "Retry attempt %d for chunk index: %d, Error: %s",
                    retries, chunk.getChunkIndex(), e.getMessage()));
            chunk.setStatus(ChunkStatus.DOWNLOAD_RETRY);
            try {
              Thread.sleep(RETRY_DELAY_MS);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw new DatabricksSQLException(
                  "Chunk download was interrupted",
                  ie,
                  DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
            }
          }
        }
      }
    } catch (Throwable t) {
      uncaughtException = t;
      throw t;
    } finally {
      if (downloadSuccessful) {
        chunk.getChunkReadyFuture().complete(null);
      } else {
        LOGGER.info(
            "Uncaught exception during chunk download. Chunk index: %d, Error: %s",
            chunk.getChunkIndex(), Arrays.toString(uncaughtException.getStackTrace()));
        // Status is set to DOWNLOAD_SUCCEEDED in the happy path. For any failure case,
        // explicitly set status to DOWNLOAD_FAILED here to ensure consistent error handling
        chunk.setStatus(ChunkStatus.DOWNLOAD_FAILED);
        chunk
            .getChunkReadyFuture()
            .completeExceptionally(
                new DatabricksSQLException(
                    "Download failed for chunk index " + chunk.getChunkIndex(),
                    DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR));
      }

      exportLatencyLog(System.currentTimeMillis() - startTime);
      DatabricksThreadContextHolder.clearAllContext();
    }

    return null;
  }
}
