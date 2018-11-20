package com.redhat.dsevosty.common.model;

public interface Versionable {
    public boolean isVersionEqual(Versionable other);
    public boolean isVersionSet();
    public void setVersion();
    public String versionAsString();
}