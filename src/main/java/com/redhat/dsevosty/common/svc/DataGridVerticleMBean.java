package com.redhat.dsevosty.common.svc;

/* 
  https://vertx.io/docs/vertx-dropwizard-metrics/java/
*/

public interface DataGridVerticleMBean extends CommonVerticleMBean {

  public void createCacheManager();
  public void destroyCacheManger();
  public void setHotrodServerHost(String host);
  public String getHotrodServerHost();
  public void setHotrodServerPort(int port);
  public int getHotrodServerPort();

  // public void registerRestApi();
  // public void unregisterRestApi();
}