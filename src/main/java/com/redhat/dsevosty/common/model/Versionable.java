package com.redhat.dsevosty.common.model;

public interface Versionable {
    public boolean isVersionEqual(Object other);
    public boolean isVersionSet();
    public void setVersion();
    public String versionAsString();
}