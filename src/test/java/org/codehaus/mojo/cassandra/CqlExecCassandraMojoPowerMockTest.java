package org.codehaus.mojo.cassandra;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;
import org.codehaus.plexus.util.IOUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.internal.stubbing.answers.DoesNothing;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.codehaus.mojo.cassandra.CqlExecOperationTest.CQL_STATEMENT;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@PrepareForTest({FileReader.class, IOUtil.class, Utils.class})
public class CqlExecCassandraMojoPowerMockTest {

    @Rule
    public PowerMockRule rule = new PowerMockRule();

    @Mock
    private Log log;

    @Mock
    private Cassandra.Client client;

    private CqlExecCassandraMojoBuilder builder;

    @Before
    public void createCqlExecCassandraMojoBuilder() {
        this.builder = new CqlExecCassandraMojoBuilder(log);
    }

    @Test
    public void should_fail_if_file_not_found_occurs_when_reading_cql_script() {
        CqlExecCassandraMojo cqlExecCassandraMojo = builder.cqlScript(file("emptyfile.cql")).build();
        mockToThrows(new FileNotFoundException());

        try {
            cqlExecCassandraMojo.execute();
            fail();
        } catch (MojoExecutionException e) {
            assertThat(e.getMessage(), allOf(startsWith("Cql file '"), endsWith("emptyfile.cql' was deleted before I could read it")));
        } catch (MojoFailureException e) {
            fail();
        }
    }

    @Test
    public void should_fail_if_io_error_occurs_when_reading_cql_script() {
        CqlExecCassandraMojo cqlExecCassandraMojo = builder.cqlScript(file("emptyfile.cql")).build();
        mockToThrows(new IOException());

        try {
            cqlExecCassandraMojo.execute();
            fail();
        } catch (MojoExecutionException e) {
            assertEquals("Could not parse or load cql file", e.getMessage());
        } catch (MojoFailureException e) {
            fail();
        }
    }

    @Test
    public void should_execute_one_cql_statement() {
        CqlExecCassandraMojo cqlExecCassandraMojo = builder.rpcAddress("localhost").rpcPort(9160).cqlStatement(CQL_STATEMENT).build();
        ArgumentCaptor<ThriftApiOperation> operation = mockThriftExecution();

        try {
            cqlExecCassandraMojo.execute();

            assertEquals("localhost", operation.getValue().getRpcAddress());
            assertEquals(9160, operation.getValue().getRpcPort());
            assertEquals(CQL_STATEMENT, extractCqlStatement(operation.getValue()));
        } catch (MojoExecutionException e) {
            fail();
        } catch (MojoFailureException e) {
            fail();
        }
    }

    @Test
    public void should_execute_many_cql_statements() {
        CqlExecCassandraMojo cqlExecCassandraMojo = builder.cqlStatement(CQL_STATEMENT + ";" + CQL_STATEMENT).build();
        ArgumentCaptor<ThriftApiOperation> operation = mockThriftExecution();

        try {
            cqlExecCassandraMojo.execute();

            assertArrayEquals(new String[]{CQL_STATEMENT, CQL_STATEMENT}, extractCqlStatements(operation.getAllValues()));
        } catch (MojoExecutionException e) {
            fail();
        } catch (MojoFailureException e) {
            fail();
        }
    }

    @Test
    public void should_use_default_cql_version() {
        CqlExecCassandraMojo cqlExecCassandraMojo = builder.cqlStatement(CQL_STATEMENT).build();
        ArgumentCaptor<ThriftApiOperation> operation = mockThriftExecution();

        try {
            cqlExecCassandraMojo.execute();

            assertEquals("2.0.0", operation.getValue().getCqlVersion());
        } catch (MojoExecutionException e) {
            fail();
        } catch (MojoFailureException e) {
            fail();
        }
    }

    @Test
    public void should_use_custom_cql_version() {
        CqlExecCassandraMojo cqlExecCassandraMojo = builder.cqlVersion("3.0.0").cqlStatement(CQL_STATEMENT).build();
        ArgumentCaptor<ThriftApiOperation> operation = mockThriftExecution();

        try {
            cqlExecCassandraMojo.execute();

            assertEquals("3.0.0", operation.getValue().getCqlVersion());
        } catch (MojoExecutionException e) {
            fail();
        } catch (MojoFailureException e) {
            fail();
        }
    }

    @Test
    public void should_use_custom_keyspace() {
        CqlExecCassandraMojo cqlExecCassandraMojo = builder.keyspace("identifier").cqlStatement(CQL_STATEMENT).build();
        ArgumentCaptor<ThriftApiOperation> operation = mockThriftExecution();

        try {
            cqlExecCassandraMojo.execute();

            assertEquals("identifier", operation.getValue().getKeyspace());
        } catch (MojoExecutionException e) {
            fail();
        } catch (MojoFailureException e) {
            fail();
        }
    }

    @Test
    public void should_fail_when_request_fails() {
        CqlExecCassandraMojo cqlExecCassandraMojo = builder.cqlStatement(CQL_STATEMENT).build();
        mockThriftExecutionWith(new ThrowsException(new ThriftApiExecutionException(new InvalidRequestException("bad statement"))));

        try {
            cqlExecCassandraMojo.execute();
            fail();
        } catch (MojoExecutionException e) {
            assertEquals("There was a problem calling Apache Cassandra's Thrift API. Details: ", e.getMessage());
        } catch (MojoFailureException e) {
            fail();
        }
    }

    private String[] extractCqlStatements(List<ThriftApiOperation> operations) {
        ArrayList<String> cqlStatements = new ArrayList<String>();
        for (ThriftApiOperation operation : operations) {
            cqlStatements.add(extractCqlStatement(operation));
        }
        return cqlStatements.toArray(new String[operations.size()]);
    }

    private String extractCqlStatement(ThriftApiOperation operation) {
        try {
            return new String(((ByteBuffer) CqlExecCassandraMojo.CqlExecOperation.class.getDeclaredField("statementBuf").get(operation)).array());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private ArgumentCaptor<ThriftApiOperation> mockThriftExecution() {
        return mockThriftExecutionWith(new DoesNothing());
    }

    private ArgumentCaptor<ThriftApiOperation> mockThriftExecutionWith(Answer<Object> answer) {
        mockStatic(Utils.class);
        ArgumentCaptor<ThriftApiOperation> operation = ArgumentCaptor.forClass(ThriftApiOperation.class);
        try {
            when(Utils.class, "executeThrift", operation.capture()).thenAnswer(answer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return operation;
    }

    private void mockToThrows(Throwable throwable) {
        try {
            mockStatic(IOUtil.class);
            when(IOUtil.toString(any(FileReader.class))).thenThrow(throwable);
        } catch (IOException e) {
            fail();
        }
    }

    private File file(String name) {
        try {
            return new File(getClass().getResource(name).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
