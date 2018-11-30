package com.redhat.dsevosty.common.svc;

public interface CommonVerticleMBean {
  public String getServiceName();

  public void createHttpServer();
  public void destroyHttpServer();
  public String getHttpServerHost();
  public void setHttpServerHost(String host);
  public int getHttpServerPort();
  public void setHttpServerPort(int port);

  public void registerManagementRestApi();
  public void unregisterManagementRestApi();

  public void registerEventBusHandler();
  public void unregisterEventBusHandler();
}
