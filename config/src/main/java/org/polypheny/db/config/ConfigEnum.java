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
 */

package org.polypheny.db.config;


import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;
import com.typesafe.config.ConfigException;
import java.util.EnumSet;
import java.util.Set;
import org.polypheny.db.config.exception.ConfigRuntimeException;


public class ConfigEnum extends Config {

    @SerializedName("values")
    private final Set<Enum> enumValues;
    private Enum value;


    public ConfigEnum( final String key, final String description, final Class enumClass, final Enum defaultValue ) {
        super( key, description );
        //noinspection unchecked
        enumValues = ImmutableSet.copyOf( EnumSet.allOf( enumClass ) );
        setEnum( defaultValue );
        this.webUiFormType = WebUiFormType.SELECT;
    }


    @Override
    public Set<Enum> getEnumValues() {
        return enumValues;
    }


    @Override
    public Enum getEnum() {
        return value;
    }


    @Override
    public boolean setEnum( final Enum value ) {
        if ( enumValues.contains( value ) ) {
            if ( validate( value ) ) {
                this.value = value;
                notifyConfigListeners();
                return true;
            } else {
                return false;
            }
        } else {
            throw new ConfigRuntimeException( "This enum in the specified enum class." );
        }
    }


    @Override
    void setValueFromFile( final com.typesafe.config.Config conf ) {
        try {
            setEnum( getByString( conf.getString( this.getKey() ) ) );
        } catch ( ConfigException.Missing e ) {
            // This should have been checked before!
            throw new ConfigRuntimeException( "No config with this key found in the configuration file." );
        } catch ( ConfigException.WrongType e ) {
            throw new ConfigRuntimeException( "The value in the config file has a type which is incompatible with this config element." );
        }

    }


    @Override
    public boolean parseStringAndSetValue( String value ) {
        return setEnum( getByString( value ) );
    }


    private Enum getByString( String str ) throws ConfigRuntimeException {
        for ( Enum e : enumValues ) {
            if ( str.equalsIgnoreCase( e.name() ) ) {
                return e;
            }
        }
        throw new ConfigRuntimeException( "No enum with name \"" + str + "\" found in the set of valid enums." );
    }
}
