package org.codehaus.mojo.cassandra;

import org.apache.maven.plugin.logging.Log;

class CqlExecOperationBuilder {
    private final CqlExecCassandraMojo.CqlExecOperation cqlExecOperation;
    private CqlExecCassandraMojo cqlExecCassandraMojo;

    CqlExecOperationBuilder() {
        cqlExecCassandraMojo = new CqlExecCassandraMojo();
        cqlExecOperation = cqlExecCassandraMojo.new CqlExecOperation("", 0, CqlExecOperationTest.CQL_STATEMENT);
    }

    CqlExecOperationBuilder log(Log log) {
        cqlExecCassandraMojo.setLog(log);
        return this;
    }

    CqlExecOperationBuilder cqlVersion(String cqlVersion) {
        cqlExecOperation.setCqlVersion(cqlVersion);
        return this;
    }

    CqlExecCassandraMojo.CqlExecOperation build() {
        return cqlExecOperation;
    }
}
