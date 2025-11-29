/*
 * Licensed to the Apache Software Foundation (ASF) ...
 */

package org.apache.shardingsphere.proxy.frontend.postgresql.command.query.extended;

import lombok.SneakyThrows;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.PostgreSQLColumnType;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.resource.unit.StorageUnit;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.sqltranslator.rule.SQLTranslatorRule;
import org.apache.shardingsphere.sqltranslator.rule.builder.DefaultSQLTranslatorRuleConfigurationBuilder;
import org.apache.shardingsphere.test.infra.framework.extension.mock.AutoMockExtension;
import org.apache.shardingsphere.test.infra.framework.extension.mock.StaticMockSettings;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
@StaticMockSettings(ProxyContext.class)
class PostgreSQLUDTMetadataTest {
    
    private final DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "PostgreSQL");
    
    @Mock
    private ConnectionSession connectionSession;
    
    @Mock
    private ContextManager contextManager;
    
    @Test
    void assertVarbitColumnTypeName() {
        ShardingSphereDatabase database = mockDatabaseWithColumns(
                new ShardingSphereColumn("flags", Types.BIT, false, false, "varbit", false, true, false, false));
        
        prepareContext(database);
        
        ShardingSphereColumn actualColumn = database.getSchema("public").getTable("t").getColumn("flags");
        String actualTypeName = actualColumn.getTypeName();
        
        assertThat(actualTypeName, is("varbit"));
    }
    
    @Test
    void assertCustomUDTColumnTypeName() {
        ShardingSphereDatabase database = mockDatabaseWithColumns(
                new ShardingSphereColumn("vector_col", Types.OTHER, false, false, "vector", false, true, false, false));
        
        prepareContext(database);
        
        ShardingSphereColumn actualColumn = database.getSchema("public").getTable("t").getColumn("vector_col");
        String actualTypeName = actualColumn.getTypeName();
        
        assertThat(actualTypeName, is("vector"));
    }
    
    @Test
    void assertMixedColumnsTypeName() {
        ShardingSphereDatabase database = mockDatabaseWithColumns(
                new ShardingSphereColumn("id", Types.INTEGER, false, false, "int4", false, true, false, false),
                new ShardingSphereColumn("json_data", Types.OTHER, false, false, "jsonb", false, true, false, false),
                new ShardingSphereColumn("my_enum", Types.OTHER, false, false, "my_enum_type", false, true, false, false)
        );
        
        prepareContext(database);
        
        String actualIdType = database.getSchema("public").getTable("t").getColumn("id").getTypeName();
        String actualJsonType = database.getSchema("public").getTable("t").getColumn("json_data").getTypeName();
        String actualEnumType = database.getSchema("public").getTable("t").getColumn("my_enum").getTypeName();
        
        assertThat(actualIdType, is("int4"));
        assertThat(actualJsonType, is("jsonb"));
        assertThat(actualEnumType, is("my_enum_type"));
    }
    
    // ------- helper methods -------
    
    private void prepareContext(final ShardingSphereDatabase database) {
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        when(contextManager.getMetaDataContexts().getMetaData().containsDatabase("db")).thenReturn(true);
        when(contextManager.getMetaDataContexts().getMetaData().getDatabase("db")).thenReturn(database);
        when(connectionSession.getCurrentDatabaseName()).thenReturn("db");
    }
    
    private ShardingSphereDatabase mockDatabaseWithColumns(final ShardingSphereColumn... columns) {
        ShardingSphereDatabase database = org.mockito.Mockito.mock(ShardingSphereDatabase.class, RETURNS_DEEP_STUBS);
        
        when(database.containsSchema("public")).thenReturn(true);
        when(database.getSchema("public").containsTable("t")).thenReturn(true);
        when(database.getSchema("public").getTable("t").getAllColumns()).thenReturn(Arrays.asList(columns));
        
        StorageUnit storageUnit = org.mockito.Mockito.mock(StorageUnit.class, RETURNS_DEEP_STUBS);
        when(storageUnit.getStorageType()).thenReturn(databaseType);
        when(database.getResourceMetaData().getStorageUnits()).thenReturn(Collections.singletonMap("ds_0", storageUnit));
        
        RuleMetaData globalRuleMetaData = new RuleMetaData(Collections.singleton(new SQLTranslatorRule(new DefaultSQLTranslatorRuleConfigurationBuilder().build())));
        when(database.getRuleMetaData()).thenReturn(new RuleMetaData(Collections.emptyList()));
        when(database.getResourceMetaData().getAllInstanceDataSourceNames()).thenReturn(Collections.singletonList("ds_0"));
        when(contextManager.getMetaDataContexts().getMetaData().getGlobalRuleMetaData()).thenReturn(globalRuleMetaData);
        
        return database;
    }
}
