package org.codehaus.mojo.cassandra;

import org.apache.cassandra.thrift.*;
import org.apache.maven.plugin.logging.Log;
import org.apache.thrift.TException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CqlExecOperationTest {

    final static String CQL_STATEMENT = "CREATE KEYSPACE identifier WITH replication = {'class': 'SimpleStrategy'}";

    @Mock
    private Cassandra.Client client;

    @Test
    public void should_execute_cql_query_with_client() {
        CqlExecCassandraMojo.CqlExecOperation cqlExecOperation = new CqlExecOperationBuilder().build();
        CqlRow row = new CqlRow();
        CqlResult result = new CqlResult();
        result.addToRows(row);
        try {
            when(client.execute_cql_query(eq(ByteBuffer.wrap(CQL_STATEMENT.getBytes())), eq(Compression.NONE))).thenReturn(result);
        } catch (TException e) {
            throw new RuntimeException(e);
        }

        cqlExecOperation.executeOperation(client);

        assertEquals(row, cqlExecOperation.next());
    }

    @Test
    public void should_execute_cql3_query_with_client() {
        CqlExecCassandraMojo.CqlExecOperation cqlExecOperation = new CqlExecOperationBuilder().cqlVersion("3.0.0").build();
        CqlRow row = new CqlRow();
        CqlResult result = new CqlResult();
        result.addToRows(row);
        try {
            when(client.execute_cql3_query(eq(ByteBuffer.wrap(CQL_STATEMENT.getBytes())), eq(Compression.NONE), eq(ConsistencyLevel.ONE))).thenReturn(result);
        } catch (TException e) {
            throw new RuntimeException(e);
        }

        cqlExecOperation.executeOperation(client);

        assertEquals(row, cqlExecOperation.next());
    }

    @Test
    public void should_fail_when_execution_fails() {
        Log log = mock(Log.class);
        CqlExecCassandraMojo.CqlExecOperation cqlExecOperation = new CqlExecOperationBuilder().cqlVersion("3.0.0").log(log).build();
        CqlRow row = new CqlRow();
        CqlResult result = new CqlResult();
        result.addToRows(row);
        InvalidRequestException cause = new InvalidRequestException("why");
        try {
            when(client.execute_cql3_query(eq(ByteBuffer.wrap(CQL_STATEMENT.getBytes())), eq(Compression.NONE), eq(ConsistencyLevel.ONE))).thenThrow(cause);
        } catch (TException e) {
            throw new RuntimeException(e);
        }

        try {
            cqlExecOperation.executeOperation(client);

            fail();
        } catch (ThriftApiExecutionException e) {
            verify(log).debug(eq(CQL_STATEMENT));
            assertEquals("There was a problem calling Apache Cassandra's Thrift API. Details: ", e.getMessage());
            assertEquals(cause, e.getCause());
        }
    }
}
