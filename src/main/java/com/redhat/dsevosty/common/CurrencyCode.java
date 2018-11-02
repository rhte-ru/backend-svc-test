package com.redhat.dsevosty.common;

public enum CurrencyCode {
  // Currency codes ISO4217, also
  EUR(978, "EUR", "Euro Member Countries"), 
  @Deprecated RUR(810, "RUR", "Russian Ruble"), 
  RUB(643, "RUB", "Российский рубль"), 
  USD(840, "USD", "United States Dollar");

  public final int key;
  public final String code;
  public final String desc;

  private CurrencyCode(int key, String code, String desc) {
    this.key = key;
    this.code = code;
    this.desc = desc;
  }
}