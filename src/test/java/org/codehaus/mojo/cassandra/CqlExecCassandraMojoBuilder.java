package org.codehaus.mojo.cassandra;

import org.apache.maven.plugin.logging.Log;

import java.io.File;

class CqlExecCassandraMojoBuilder {
    private final CqlExecCassandraMojo cqlExecCassandraMojo;

    CqlExecCassandraMojoBuilder() {
        this.cqlExecCassandraMojo = new CqlExecCassandraMojo();
    }

    CqlExecCassandraMojoBuilder(Log log) {
        this();
        this.cqlExecCassandraMojo.setLog(log);
    }

    CqlExecCassandraMojoBuilder skip() {
        cqlExecCassandraMojo.skip = true;
        return this;
    }

    CqlExecCassandraMojoBuilder comparator(String comparator) {
        cqlExecCassandraMojo.comparator = comparator;
        return this;
    }

    CqlExecCassandraMojoBuilder keyValidator(String keyValidator) {
        cqlExecCassandraMojo.keyValidator = keyValidator;
        return this;
    }

    CqlExecCassandraMojoBuilder defaultValidator(String defaultValidator) {
        cqlExecCassandraMojo.defaultValidator = defaultValidator;
        return this;
    }

    CqlExecCassandraMojoBuilder cqlScript(File cqlScript) {
        cqlExecCassandraMojo.cqlScript = cqlScript;
        return this;
    }

    CqlExecCassandraMojoBuilder cqlStatement(String cqlStatement) {
        cqlExecCassandraMojo.cqlStatement = cqlStatement;
        return this;
    }

    CqlExecCassandraMojoBuilder rpcAddress(String rpcAddress) {
        cqlExecCassandraMojo.rpcAddress = rpcAddress;
        return this;
    }

    CqlExecCassandraMojoBuilder cqlVersion(String cqlVersion) {
        cqlExecCassandraMojo.cqlVersion = cqlVersion;
        return this;
    }

    CqlExecCassandraMojoBuilder rpcPort(int rpcPort) {
        cqlExecCassandraMojo.rpcPort = rpcPort;
        return this;
    }

    CqlExecCassandraMojoBuilder keyspace(String keyspace) {
        cqlExecCassandraMojo.keyspace = keyspace;
        return this;
    }

    CqlExecCassandraMojo build() {
        return cqlExecCassandraMojo;
    }
}
