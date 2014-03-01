package de.tu_berlin.citlab.storm.operators;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.IndifferentAccessMap;

import java.io.Serializable;
import java.util.List;


public class StaticTuple extends IndifferentAccessMap implements Tuple, Serializable {
    private Values values;
    private Fields fields;

    public StaticTuple(Fields fields, Values values ){
        assert( fields.size() == values.size() );

        this.fields = fields;
        this.values = values;
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public int fieldIndex(String field) {
        return getFields().fieldIndex(field);
    }

    @Override
    public boolean contains(String field) {
        return getFields().contains(field);
    }

    @Override
    public Object getValue(int i) {
        return values.get(i);
    }

    @Override
    public String getString(int i) {
        return (String) values.get(i);
    }

    @Override
    public Integer getInteger(int i) {
        return (Integer) values.get(i);
    }

    @Override
    public Long getLong(int i) {
        return (Long) values.get(i);
    }

    @Override
    public Boolean getBoolean(int i) {
        return (Boolean) values.get(i);
    }

    @Override
    public Short getShort(int i) {
        return (Short) values.get(i);
    }

    @Override
    public Byte getByte(int i) {
        return (Byte) values.get(i);
    }

    @Override
    public Double getDouble(int i) {
        return (Double) values.get(i);
    }

    @Override
    public Float getFloat(int i) {
        return (Float) values.get(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return (byte[]) values.get(i);
    }

    @Override
    public Object getValueByField(String field) {
        return values.get(fieldIndex(field));
    }

    @Override
    public String getStringByField(String field) {
        return (String) values.get(fieldIndex(field));
    }

    @Override
    public Integer getIntegerByField(String field) {
        return (Integer) values.get(fieldIndex(field));
    }

    @Override
    public Long getLongByField(String field) {
        return (Long) values.get(fieldIndex(field));
    }

    @Override
    public Boolean getBooleanByField(String field) {
        return (Boolean) values.get(fieldIndex(field));
    }


    @Override
    public Short getShortByField(String field) {
        return (Short) values.get(fieldIndex(field));
    }

    @Override
    public Byte getByteByField(String field) {
        return (Byte) values.get(fieldIndex(field));
    }

    @Override
    public Double getDoubleByField(String field) {
        return (Double) values.get(fieldIndex(field));
    }

    @Override
    public Float getFloatByField(String field) {
        return (Float) values.get(fieldIndex(field));
    }

    @Override
    public byte[] getBinaryByField(String field) {
        return (byte[]) values.get(fieldIndex(field));
    }

    @Override
    public List<Object> getValues() {
        return values;
    }

    @Override
    public Fields getFields() {
        return fields;
    }

    @Override
    public List<Object> select(Fields selector) {
        return getFields().select(selector, values);
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamid() {
        return null;
    }

    @Override
    public String getSourceComponent() {
        return null;
    }

    @Override
    public int getSourceTask() {
        return 0;
    }

    @Override
    public String getSourceStreamId() {
        return null;
    }

    @Override
    public MessageId getMessageId() {
        return null;
    }

    @Override
    public String toString() {
        return "static tuple: source: " + getSourceComponent() + ":no source" + ", stream: no-stream" + ", id: no-id"+ ", " + values.toString();
    }

}
