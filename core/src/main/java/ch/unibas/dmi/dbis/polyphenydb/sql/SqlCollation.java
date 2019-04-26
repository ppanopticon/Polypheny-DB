/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbException;
import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbResource;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.Glossary;
import ch.unibas.dmi.dbis.polyphenydb.util.SaffronProperties;
import ch.unibas.dmi.dbis.polyphenydb.util.SerializableCharset;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Locale;


/**
 * A <code>SqlCollation</code> is an object representing a <code>Collate</code> statement. It is immutable.
 */
public class SqlCollation implements Serializable {

    public static final SqlCollation COERCIBLE = new SqlCollation( Coercibility.COERCIBLE );
    public static final SqlCollation IMPLICIT = new SqlCollation( Coercibility.IMPLICIT );


    /**
     * <blockquote>A &lt;character value expression&gt; consisting of a column reference has the coercibility characteristic Implicit, with collating
     * sequence as defined when the column was created. A &lt;character value expression&gt; consisting of a value other than a column (e.g., a host
     * variable or a literal) has the coercibility characteristic Coercible, with the default collation for its character repertoire. A &lt;character
     * value expression&gt; simply containing a &lt;collate clause&gt; has the coercibility characteristic Explicit, with the collating sequence
     * specified in the &lt;collate clause&gt;.</blockquote>
     *
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3
     */
    public enum Coercibility {
        /**
         * Strongest coercibility.
         */
        EXPLICIT,
        IMPLICIT,
        COERCIBLE,
        /**
         * Weakest coercibility.
         */
        NONE
    }


    protected final String collationName;
    protected final SerializableCharset wrappedCharset;
    protected final Locale locale;
    protected final String strength;
    private final Coercibility coercibility;


    /**
     * Creates a Collation by its name and its coercibility
     *
     * @param collation Collation specification
     * @param coercibility Coercibility
     */
    public SqlCollation( String collation, Coercibility coercibility ) {
        this.coercibility = coercibility;
        SqlParserUtil.ParsedCollation parseValues = SqlParserUtil.parseCollation( collation );
        Charset charset = parseValues.getCharset();
        this.wrappedCharset = SerializableCharset.forCharset( charset );
        locale = parseValues.getLocale();
        strength = parseValues.getStrength();
        String c = charset.name().toUpperCase( Locale.ROOT ) + "$" + locale.toString();
        if ( (strength != null) && (strength.length() > 0) ) {
            c += "$" + strength;
        }
        collationName = c;
    }


    /**
     * Creates a SqlCollation with the default collation name and the given coercibility.
     *
     * @param coercibility Coercibility
     */
    public SqlCollation( Coercibility coercibility ) {
        this( SaffronProperties.INSTANCE.defaultCollation().get(), coercibility );
    }


    public boolean equals( Object o ) {
        return this == o
                || o instanceof SqlCollation
                && collationName.equals( ((SqlCollation) o).collationName );
    }


    @Override
    public int hashCode() {
        return collationName.hashCode();
    }


    /**
     * Returns the collating sequence (the collation name) and the coercibility for the resulting value of a dyadic operator.
     *
     * @param col1 first operand for the dyadic operation
     * @param col2 second operand for the dyadic operation
     * @return the resulting collation sequence. The "no collating sequence" result is returned as null.
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3 Table 2
     */
    public static SqlCollation getCoercibilityDyadicOperator( SqlCollation col1, SqlCollation col2 ) {
        return getCoercibilityDyadic( col1, col2 );
    }


    /**
     * Returns the collating sequence (the collation name) and the coercibility for the resulting value of a dyadic operator.
     *
     * @param col1 first operand for the dyadic operation
     * @param col2 second operand for the dyadic operation
     * @return the resulting collation sequence
     * @throws PolyphenyDbException from {@link PolyphenyDbResource#invalidCompare} or {@link PolyphenyDbResource#differentCollations} if no collating sequence can be deduced
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3 Table 2
     */
    public static SqlCollation getCoercibilityDyadicOperatorThrows( SqlCollation col1, SqlCollation col2 ) {
        SqlCollation ret = getCoercibilityDyadic( col1, col2 );
        if ( null == ret ) {
            throw RESOURCE.invalidCompare(
                    col1.collationName,
                    "" + col1.coercibility,
                    col2.collationName,
                    "" + col2.coercibility ).ex();
        }
        return ret;
    }


    /**
     * Returns the collating sequence (the collation name) to use for the resulting value of a comparison.
     *
     * @param col1 first operand for the dyadic operation
     * @param col2 second operand for the dyadic operation
     * @return the resulting collation sequence. If no collating sequence could be deduced throws a {@link PolyphenyDbResource#invalidCompare}
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3 Table 3
     */
    public static String getCoercibilityDyadicComparison( SqlCollation col1, SqlCollation col2 ) {
        return getCoercibilityDyadicOperatorThrows( col1, col2 ).collationName;
    }


    /**
     * Returns the result for {@link #getCoercibilityDyadicComparison} and {@link #getCoercibilityDyadicOperator}.
     */
    protected static SqlCollation getCoercibilityDyadic( SqlCollation col1, SqlCollation col2 ) {
        assert null != col1;
        assert null != col2;
        final Coercibility coercibility1 = col1.getCoercibility();
        final Coercibility coercibility2 = col2.getCoercibility();
        switch ( coercibility1 ) {
            case COERCIBLE:
                switch ( coercibility2 ) {
                    case COERCIBLE:
                        return col2;
                    case IMPLICIT:
                        return col2;
                    case NONE:
                        return null;
                    case EXPLICIT:
                        return col2;
                    default:
                        throw Util.unexpected( coercibility2 );
                }
            case IMPLICIT:
                switch ( coercibility2 ) {
                    case COERCIBLE:
                        return col1;
                    case IMPLICIT:
                        if ( col1.collationName.equals( col2.collationName ) ) {
                            return col2;
                        }
                        return null;
                    case NONE:
                        return null;
                    case EXPLICIT:
                        return col2;
                    default:
                        throw Util.unexpected( coercibility2 );
                }
            case NONE:
                switch ( coercibility2 ) {
                    case COERCIBLE:
                    case IMPLICIT:
                    case NONE:
                        return null;
                    case EXPLICIT:
                        return col2;
                    default:
                        throw Util.unexpected( coercibility2 );
                }
            case EXPLICIT:
                switch ( coercibility2 ) {
                    case COERCIBLE:
                    case IMPLICIT:
                    case NONE:
                        return col1;
                    case EXPLICIT:
                        if ( col1.collationName.equals( col2.collationName ) ) {
                            return col2;
                        }
                        throw RESOURCE.differentCollations( col1.collationName, col2.collationName ).ex();
                    default:
                        throw Util.unexpected( coercibility2 );
                }
            default:
                throw Util.unexpected( coercibility1 );
        }
    }


    public String toString() {
        return "COLLATE " + collationName;
    }


    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "COLLATE" );
        writer.identifier( collationName );
    }


    public Charset getCharset() {
        return wrappedCharset.getCharset();
    }


    public final String getCollationName() {
        return collationName;
    }


    public final SqlCollation.Coercibility getCoercibility() {
        return coercibility;
    }
}