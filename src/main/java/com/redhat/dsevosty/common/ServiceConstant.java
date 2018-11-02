package com.redhat.dsevosty.common;

public enum ServiceConstant {
  SERVICE_NAMESPACE("service.namespace", ""), 
  SERVICE_HTTP_LISTEN_ADDRESS("service.http.listen.address", "127.0.0.1"),
  SERVICE_HTTP_LISTEN_PORT("service.http.listen.port", "8080"),
  SERVICE_HTTP_REMOTE_ADDRESS("service.http.remote.address", "127.0.0.1"),
  SERVICE_HTTP_REMOTE_PORT("service.http.remote.port", "8080"),
  SERVICE_HTTP_MANAGEMMENT_SUFFIX("service.management.http.address", "mgmt"),
  SERVICE_HTTP_API_SUFFIX("service.api.http.address", "api"),
  SERVICE_JDG_REMOTE_ADDRESS("service.jdg.remote.address", "127.0.0.1"),
  SERVICE_JDG_REMOTE_PORT("service.jdg.remote.port", "11222"),
  SERVICE_EVENTBUS_PREFIX("vertx.eventbus.prefix", "com.redhat.dsevosty.eventbus"),
  SERVICE_OPERATION("service.operation","")
  ;

  // private static final String PACKAGE_NAME = "com.redhat.dsevosty";
  public final String key;
  public final String value;

  private ServiceConstant(String key, String value) {
    this.key = key;
    this.value = value;
  }
}