/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.spi.commons.value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.jackrabbit.spi.QValue;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.Path;
import org.apache.jackrabbit.util.ISO8601;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFormatException;
import java.util.Calendar;
import java.util.TimeZone;
import java.math.BigDecimal;
import java.net.URI;

/**
 * <code>AbstractQValue</code>...
 */
public abstract class AbstractQValue implements QValue {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(AbstractQValue.class);

    protected Object val;
    protected final int type;

    /**
     * Create a new <code>AbstractQValue</code>.
     *
     * @param value The value.
     * @param type The property type.
     * @throws IllegalArgumentException if the passed <code>value</code>
     * is <code>null</code>.
     */
    protected AbstractQValue(Object value, int type) {
        if (value == null) {
            throw new IllegalArgumentException("null value");
        }
        val = value;
        this.type = type;
    }

    /**
     * Create a new <code>AbstractQValue</code>.
     *
     * @param value
     * @param type
     * @throws IllegalArgumentException if the passed <code>value</code>
     * is <code>null</code> or if the <code>type</code> is neither STRING nor
     * REFERENCE/WEAKREFERENCE.
     */
    protected AbstractQValue(String value, int type) {
        if (value == null) {
            throw new IllegalArgumentException("null value");
        }
        if (!(type == PropertyType.STRING || type == PropertyType.REFERENCE || type == PropertyType.WEAKREFERENCE)) {
            throw new IllegalArgumentException();
        }
        val = value;
        this.type = type;
    }

    /**
     * Create a new <code>AbstractQValue</code>.
     *
     * @param value
     * @throws IllegalArgumentException if the passed <code>value</code>
     * is <code>null</code>.
     */
    protected AbstractQValue(Long value) {
        this(value, PropertyType.LONG);
    }

    /**
     * Create a new <code>AbstractQValue</code>.
     *
     * @param value
     * @throws IllegalArgumentException if the passed <code>value</code>
     * is <code>null</code>.
     */
    protected AbstractQValue(Double value) {
        this(value, PropertyType.DOUBLE);
    }

    /**
     * Create a new <code>AbstractQValue</code>.
     *
     * @param value
     * @throws IllegalArgumentException if the passed <code>value</code>
     * is <code>null</code>.
     */
    protected AbstractQValue(Boolean value) {
        this(value, PropertyType.BOOLEAN);
    }

    /**
     * Create a new <code>AbstractQValue</code>.
     *
     * @param value
     * @throws IllegalArgumentException if the passed <code>value</code>
     * is <code>null</code>.
     */
    protected AbstractQValue(Calendar value) {
        this(value, PropertyType.DATE);
    }

    /**
     * Create a new <code>AbstractQValue</code>.
     *
     * @param value
     * @throws IllegalArgumentException if the passed <code>value</code>
     * is <code>null</code>.
     */
    protected AbstractQValue(Name value) {
        this(value, PropertyType.NAME);
    }

    /**
     * Create a new <code>AbstractQValue</code>.
     *
     * @param value
     * @throws IllegalArgumentException if the passed <code>value</code>
     * is <code>null</code>.
     */
    protected AbstractQValue(Path value) {
        this(value, PropertyType.PATH);
    }

    /**
     * Create a new <code>AbstractQValue</code>.
     *
     * @param value
     * @throws IllegalArgumentException if the passed <code>value</code>
     * is <code>null</code>.
     */
    protected AbstractQValue(BigDecimal value) {
        this(value, PropertyType.DECIMAL);
    }

    /**
     * Create a new <code>AbstractQValue</code>.
     *
     * @param value
     * @throws IllegalArgumentException if the passed <code>value</code>
     * is <code>null</code>.
     */
    protected AbstractQValue(URI value) {
        this(value, PropertyType.URI);
    }

    //---------------------------------------------------------< QValue >---
    /**
     * @see QValue#getType()
     */
    public int getType() {
        return type;
    }

    /**
     * @see QValue#getLength()
     */
    public long getLength() throws RepositoryException {
        return getString().length();
    }

    /**
     * @see QValue#getName()
     */
    public Name getName() throws RepositoryException {
        if (type == PropertyType.NAME) {
            return (Name) val;
        } else {
            try {
                return AbstractQValueFactory.NAME_FACTORY.create(getString());
            } catch (IllegalArgumentException e) {
                throw new ValueFormatException("not a valid Name value: " + getString(), e);
            }
        }
    }

    /**
     * @see QValue#getCalendar()
     */
    public Calendar getCalendar() throws RepositoryException {
        if (type == PropertyType.DATE) {
            return (Calendar) ((Calendar) val).clone();
        } else if (type == PropertyType.DOUBLE) {
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+00:00"));
            cal.setTimeInMillis(((Double) val).longValue());
            return cal;
        } else if (type == PropertyType.LONG) {
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+00:00"));
            cal.setTimeInMillis((Long) val);
            return cal;
        } else if (type == PropertyType.DECIMAL) {
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+00:00"));
            cal.setTimeInMillis(((BigDecimal) val).longValue());
            return cal;
        } else {
            Calendar cal = ISO8601.parse(getString());
            if (cal == null) {
                throw new ValueFormatException("not a date string: " + getString());
            } else {
                return cal;
            }
        }
    }

    /**
     * @see QValue#getDecimal()
     */
    public BigDecimal getDecimal() throws RepositoryException {
        if (type == PropertyType.DECIMAL) {
            return (BigDecimal) val;
        } else if (type == PropertyType.DOUBLE) {
            return new BigDecimal((Double) val);
        } else if (type == PropertyType.LONG) {
            return new BigDecimal((Long) val);
        } else if (type == PropertyType.DATE) {
            return new BigDecimal(((Calendar) val).getTimeInMillis());
        } else {
            try {
                return new BigDecimal(getString());
            } catch (NumberFormatException e) {
                throw new ValueFormatException("not a valid decimal string: " + getString(), e);
            }
        }
    }

    /**
     * @see QValue#getURI()
     */
    public URI getURI() throws RepositoryException {
        if (type == PropertyType.URI) {
            return (URI) val;
        } else {
            try {
                return URI.create(getString());
            } catch (IllegalArgumentException e) {
                throw new ValueFormatException("not a valid uri: " + getString(), e);
            }
        }
    }

    /**
     * @see QValue#getDouble()
     */
    public double getDouble() throws RepositoryException {
        if (type == PropertyType.DOUBLE) {
            return (Double) val;
        } else if (type == PropertyType.LONG) {
            return ((Long) val).doubleValue();
        } else if (type == PropertyType.DATE) {
            return ((Calendar) val).getTimeInMillis();
        } else if (type == PropertyType.DECIMAL) {
            return ((BigDecimal) val).doubleValue();
        } else {
            try {
                return Double.parseDouble(getString());
            } catch (NumberFormatException ex) {
                throw new ValueFormatException("not a double: " + getString(), ex);
            }
        }
    }

    /**
     * @see QValue#getLong()
     */
    public long getLong() throws RepositoryException {
        if (type == PropertyType.LONG) {
            return (Long) val;
        } else if (type == PropertyType.DOUBLE) {
            return ((Double) val).longValue();
        } else if (type == PropertyType.DECIMAL) {
            return ((BigDecimal) val).longValue();
        } else if (type == PropertyType.DATE) {
            return ((Calendar) val).getTimeInMillis();
        } else {
            try {
                return Long.parseLong(getString());
            } catch (NumberFormatException ex) {
                throw new ValueFormatException("not a long: " + getString(), ex);
            }
        }
    }

    /**
     * @throws RepositoryException
     * @see QValue#getBoolean()
     */
    public boolean getBoolean() throws RepositoryException {
        if (type == PropertyType.BOOLEAN) {
            return ((Boolean) val).booleanValue();
        } else {
            return Boolean.valueOf(getString()).booleanValue();
        }
    }

    /**
     * @see QValue#getPath()
     */
    public Path getPath() throws RepositoryException {
        if (type == PropertyType.PATH) {
            return (Path) val;
        } else {
            try {
                return AbstractQValueFactory.PATH_FACTORY.create(getString());
            } catch (IllegalArgumentException e) {
                throw new ValueFormatException("not a valid Path: " + getString(), e);
            }
        }
    }

    /**
     * @see QValue#discard()
     */
    public void discard() {
        // nothing to do
    }

    //---------------------------------------------------------< Object >---
    /**
     * Returns the string representation of this internal value.
     *
     * @return string representation of this internal value
     */
    public String toString() {
        return val.toString();
    }

    /**
     * Default implementation of the equals method. Subclasses may optimize
     * this e.g. by special handling for DATE properties.
     *
     * @param obj
     * @see Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof AbstractQValue) {
            AbstractQValue other = (AbstractQValue) obj;
            if (type != other.type) {
                return false;
            }
            if (PropertyType.DATE == type) {
                return ISO8601.format((Calendar) val).equals(ISO8601.format((Calendar) other.val));
            } else {
                return val.equals(other.val);
            }
        }
        return false;
    }

    /**
     * Default calculation of the hashCode. Subclasses may optimize
     * this e.g. by special handling for DATE properties.
     *
     * @return the hashCode of the internal value object.
     * @see Object#hashCode()
     */
    public int hashCode() {
        if (PropertyType.DATE == type) {
            return ISO8601.format((Calendar) val).hashCode();
        } else {
            return val.hashCode();
        }
    }
}