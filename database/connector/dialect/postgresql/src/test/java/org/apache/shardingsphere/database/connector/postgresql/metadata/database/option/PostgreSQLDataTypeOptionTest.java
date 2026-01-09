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

package org.apache.shardingsphere.database.connector.postgresql.metadata.database.option;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostgreSQLDataTypeOptionTest {
    
    @Test
    public void testLoadUDTTypesWithSingleSchema() throws SQLException {
        PostgreSQLDataTypeOption option = new PostgreSQLDataTypeOption();
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        java.sql.ResultSet mockResultSet = mock(java.sql.ResultSet.class);

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false); // Simulate no rows

        // Test with single schema - should use parameterized query
        Map<String, Integer> result = option.loadUDTTypes(mockConnection, "public");

        verify(mockConnection, times(1)).prepareStatement(anyString());
        verify(mockPreparedStatement, times(1)).setString(1, "public");
        verify(mockPreparedStatement, times(1)).executeQuery();

        // Result should be an empty map since we're mocking
        assertEquals(0, result.size());
    }
    
    @Test
    public void testLoadUDTTypesWithMultipleSchemas() throws SQLException {
        PostgreSQLDataTypeOption option = new PostgreSQLDataTypeOption();
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        java.sql.ResultSet mockResultSet = mock(java.sql.ResultSet.class);

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false); // Simulate no rows

        // Test with multiple schemas
        Map<String, Integer> result = option.loadUDTTypes(mockConnection, "schema1", "schema2", "schema3");

        verify(mockConnection, times(1)).prepareStatement(anyString());
        verify(mockPreparedStatement, times(1)).setString(1, "schema1");
        verify(mockPreparedStatement, times(1)).setString(2, "schema2");
        verify(mockPreparedStatement, times(1)).setString(3, "schema3");
        verify(mockPreparedStatement, times(1)).executeQuery();

        // Result should be an empty map since we're mocking
        assertEquals(0, result.size());
    }

    @Test
    public void testLoadUDTTypesWithEmptySchemas() throws SQLException {
        PostgreSQLDataTypeOption option = new PostgreSQLDataTypeOption();
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        java.sql.ResultSet mockResultSet = mock(java.sql.ResultSet.class);

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false); // Simulate no rows

        // Test with empty schemas array - should default to public
        Map<String, Integer> result = option.loadUDTTypes(mockConnection);

        verify(mockConnection, times(1)).prepareStatement(anyString());
        verify(mockPreparedStatement, times(1)).executeQuery();

        // Result should be an empty map since we're mocking
        assertEquals(0, result.size());
    }
}