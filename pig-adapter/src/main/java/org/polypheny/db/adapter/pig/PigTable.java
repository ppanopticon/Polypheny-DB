/*
 * Copyright 2019-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.pig;


import org.apache.pig.data.DataType;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelOptTable.ToRelContext;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.impl.AbstractTable;


/**
 * Represents a Pig relation that is created by Pig Latin <a href="https://pig.apache.org/docs/r0.13.0/basic.html#load">LOAD</a> statement.
 *
 * Only the default load function is supported at this point (PigStorage()).
 *
 * Only VARCHAR (CHARARRAY in Pig) type supported at this point.
 */
public class PigTable extends AbstractTable implements TranslatableTable {

    private final String filePath;
    private final String[] fieldNames;


    /**
     * Creates a PigTable.
     */
    public PigTable( String filePath, String[] fieldNames ) {
        this.filePath = filePath;
        this.fieldNames = fieldNames;
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        for ( String fieldName : fieldNames ) {
            // only supports CHARARRAY types for now
            final RelDataType relDataType = typeFactory.createPolyType( PigDataType.valueOf( DataType.CHARARRAY ).getSqlType() );
            final RelDataType nullableRelDataType = typeFactory.createTypeWithNullability( relDataType, true );
            // TODO (PCP)
            String physicalColumnName = fieldName;
            builder.add( fieldName, physicalColumnName, nullableRelDataType );
        }
        return builder.build();
    }


    public String getFilePath() {
        return filePath;
    }


    @Override
    public RelNode toRel( ToRelContext context, RelOptTable relOptTable ) {
        final RelOptCluster cluster = context.getCluster();
        return new PigTableScan( cluster, cluster.traitSetOf( PigRel.CONVENTION ), relOptTable );
    }
}

