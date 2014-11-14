package org.codehaus.mojo.cassandra;

import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.thrift.TException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ThriftApiExecutionExceptionTest {
    @Test
    public void should_not_give_any_details_if_there_is_no_cause() {
        ThriftApiExecutionException exception = new ThriftApiExecutionException();

        assertEquals("There was a problem calling Apache Cassandra's Thrift API. ", exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void should_not_give_some_details_if_there_is_a_message() {
        ThriftApiExecutionException exception = new ThriftApiExecutionException("additional message");

        assertEquals("There was a problem calling Apache Cassandra's Thrift API. additional message", exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void should_not_give_any_details_if_cause_is_not_handled() {
        Throwable cause = new Exception();

        ThriftApiExecutionException exception = new ThriftApiExecutionException(cause);

        assertEquals("There was a problem calling Apache Cassandra's Thrift API. Details: n/a", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    public void should_give_details_if_cause_is_unavailable_exception() {
        Throwable cause = new UnavailableException();

        ThriftApiExecutionException exception = new ThriftApiExecutionException(cause);

        assertEquals("There was a problem calling Apache Cassandra's Thrift API. Details: You do not have enough nodes up to handle the specified consistency level", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    public void should_give_details_if_cause_is_timed_out_exception() {
        Throwable cause = new TimedOutException();

        ThriftApiExecutionException exception = new ThriftApiExecutionException(cause);

        assertEquals("There was a problem calling Apache Cassandra's Thrift API. Details: Request timed out - server load may be too high, or you may be requesting too many rows for a single operation", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    public void should_give_details_if_cause_is_invalid_request_exception() {
        Throwable cause = new InvalidRequestException("why");

        ThriftApiExecutionException exception = new ThriftApiExecutionException(cause);

        assertEquals("There was a problem calling Apache Cassandra's Thrift API. Details: The request was not properly formatted why", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    public void should_give_details_if_cause_is_schema_disagreement_exception() {
        Throwable cause = new SchemaDisagreementException();

        ThriftApiExecutionException exception = new ThriftApiExecutionException(cause);

        assertEquals("There was a problem calling Apache Cassandra's Thrift API. Details: Schema versions are out of sync", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    public void should_give_details_if_cause_is_thrift_exception() {
        Throwable cause = new TException();

        ThriftApiExecutionException exception = new ThriftApiExecutionException(cause);

        assertEquals("There was a problem calling Apache Cassandra's Thrift API. Details: General Thrift Exception, ensure Apache Cassandra is running and all necessary ports are accessible", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}