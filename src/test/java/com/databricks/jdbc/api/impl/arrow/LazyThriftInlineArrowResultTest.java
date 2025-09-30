package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.TestConstants.TEST_TABLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LazyThriftInlineArrowResultTest {

  @Mock private IDatabricksSession session;
  @Mock private IDatabricksStatementInternal statement;
  private static final StatementId STATEMENT_ID = new StatementId("test_statement_id");
  private static final byte[] DUMMY_ARROW_BYTES = new byte[] {65, 66, 67};

  private TFetchResultsResp createFetchResultsResp(int rowCount, boolean hasMoreRows) {
    TSparkArrowBatch arrowBatch =
        new TSparkArrowBatch().setRowCount(rowCount).setBatch(DUMMY_ARROW_BYTES);
    TRowSet rowSet = new TRowSet().setArrowBatches(Collections.singletonList(arrowBatch));

    TGetResultSetMetadataResp metadata =
        new TGetResultSetMetadataResp().setSchema(TEST_TABLE_SCHEMA);

    TFetchResultsResp response =
        new TFetchResultsResp().setResultSetMetadata(metadata).setResults(rowSet);
    response.hasMoreRows = hasMoreRows;

    return response;
  }

  @Test
  void testConstructorInitializesCorrectly() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertEquals(-1, result.getCurrentRow());
    assertEquals(0, result.getRowCount());
    assertEquals(0, result.getTotalRowsFetched());
    assertFalse(result.hasNext());
    assertTrue(result.isCompletelyFetched());
  }

  @Test
  void testGetObjectThrowsWhenClosed() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);
    result.close();

    DatabricksSQLException exception =
        assertThrows(DatabricksSQLException.class, () -> result.getObject(0));
    assertEquals("Result is already closed", exception.getMessage());
    assertEquals(DatabricksDriverErrorCode.STATEMENT_CLOSED.name(), exception.getSQLState());
  }

  @Test
  void testGetObjectThrowsWhenBeforeFirstRow() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    DatabricksSQLException exception =
        assertThrows(DatabricksSQLException.class, () -> result.getObject(0));
    assertEquals("Cursor is before first row", exception.getMessage());
    assertEquals(DatabricksDriverErrorCode.INVALID_STATE.name(), exception.getSQLState());
  }

  @Test
  void testCloseReleasesResources() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    result.close();

    assertFalse(result.hasNext());
    assertFalse(result.next());
  }

  @Test
  void testIsCompletelyFetchedWhenNoMoreRows() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertTrue(result.isCompletelyFetched());
  }

  @Test
  void testIsCompletelyFetchedWithMoreRows() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, true);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertFalse(result.isCompletelyFetched());
  }

  @Test
  void testGetChunkCount() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertEquals(0, result.getChunkCount());
  }

  @Test
  void testHandleErrorThrowsParsingException() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    Exception testException = new IOException("Test error");
    DatabricksParsingException exception =
        assertThrows(DatabricksParsingException.class, () -> result.handleError(testException));
    assertTrue(exception.getMessage().contains("Cannot process lazy thrift inline arrow format"));
    assertEquals(
        DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR.name(), exception.getSQLState());
  }

  @Test
  void testEmptyResultSet() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertEquals(-1, result.getCurrentRow());
    assertFalse(result.hasNext());
    assertFalse(result.next());
    assertEquals(0, result.getRowCount());
    assertTrue(result.isCompletelyFetched());
  }

  @Test
  void testNullStatement() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, null, session);

    assertEquals(-1, result.getCurrentRow());
    assertEquals(0, result.getRowCount());
  }

  @Test
  void testGetCurrentRowBeforeNext() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertEquals(-1, result.getCurrentRow());
  }

  @Test
  void testGetTotalRowsFetched() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertEquals(0, result.getTotalRowsFetched());
  }

  @Test
  void testNextReturnsFalseOnEmptyResultSet() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertFalse(result.next());
  }

  @Test
  void testHasNextReturnsFalseOnEmptyResultSet() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertFalse(result.hasNext());
  }

  @Test
  void testNextReturnsFalseAfterClose() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);
    result.close();

    assertFalse(result.next());
  }

  @Test
  void testHasNextReturnsFalseAfterClose() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);
    result.close();

    assertFalse(result.hasNext());
  }

  @Test
  void testConstructorWithNullStatementUsesDefaultMaxRows() throws DatabricksSQLException {
    TFetchResultsResp initialResponse = createFetchResultsResp(0, false);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, null, session);

    assertNotNull(result);
    assertEquals(-1, result.getCurrentRow());
  }
}
