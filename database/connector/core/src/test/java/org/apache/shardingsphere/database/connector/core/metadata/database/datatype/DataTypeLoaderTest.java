/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.database.connector.core.metadata.database.datatype;

import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DataTypeLoaderTest {
    
    @Test
    public void testLoadWithUDTDisabled() throws SQLException {
        // Set system property to disable UDT discovery
        System.setProperty("shardingsphere.udt.discovery.enabled", "false");
        
        try {
            DatabaseMetaData mockDatabaseMetaData = mock(DatabaseMetaData.class);
            when(mockDatabaseMetaData.getTypeInfo()).thenReturn(mock(java.sql.ResultSet.class));
            
            DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "H2");
            
            DataTypeLoader loader = new DataTypeLoader();
            Map<String, Integer> result = loader.load(mockDatabaseMetaData, databaseType);
            
            assertNotNull(result);
            // The result should contain standard types but not UDT types (since UDT discovery is disabled)
        } finally {
            // Reset system property
            System.clearProperty("shardingsphere.udt.discovery.enabled");
        }
    }
    
    @Test
    public void testLoadWithUDTEnabled() throws SQLException {
        // Set system property to enable UDT discovery (default behavior)
        System.setProperty("shardingsphere.udt.discovery.enabled", "true");
        
        try {
            DatabaseMetaData mockDatabaseMetaData = mock(DatabaseMetaData.class);
            when(mockDatabaseMetaData.getTypeInfo()).thenReturn(mock(java.sql.ResultSet.class));
            when(mockDatabaseMetaData.getConnection()).thenReturn(mock(java.sql.Connection.class));
            
            DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "H2");
            
            DataTypeLoader loader = new DataTypeLoader();
            Map<String, Integer> result = loader.load(mockDatabaseMetaData, databaseType);
            
            assertNotNull(result);
            // The result should contain both standard types and UDT types (if any) 
        } finally {
            // Reset system property
            System.clearProperty("shardingsphere.udt.discovery.enabled");
        }
    }
    
    @Test
    public void testLoadWithDefaultBehavior() throws SQLException {
        // Test with default behavior (should be enabled)
        System.clearProperty("shardingsphere.udt.discovery.enabled");
        
        DatabaseMetaData mockDatabaseMetaData = mock(DatabaseMetaData.class);
        when(mockDatabaseMetaData.getTypeInfo()).thenReturn(mock(java.sql.ResultSet.class));
        when(mockDatabaseMetaData.getConnection()).thenReturn(mock(java.sql.Connection.class));
        
        DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "H2");
        
        DataTypeLoader loader = new DataTypeLoader();
        Map<String, Integer> result = loader.load(mockDatabaseMetaData, databaseType);
        
        assertNotNull(result);
        // Test that it works with default behavior
    }
}