package br.com.datastreambrasil;

public final class FieldType {
    private String field;
    private String type;
    private String name;

    public FieldType(String field, String type, String name) {
        this.field = field;
        this.type = type;
        this.name = name;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
