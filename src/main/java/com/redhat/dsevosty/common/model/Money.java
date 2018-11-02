package com.redhat.dsevosty.common.model;

import java.math.BigDecimal;

import com.redhat.dsevosty.common.CurrencyCode;

public class Money {
  private BigDecimal amount;
  private CurrencyCode currency;

  public Money() {
    amount = new BigDecimal("0.00");
    currency = CurrencyCode.RUB;
  }

  public Money(BigDecimal amount, CurrencyCode currency) {
    this.amount = amount;
    this.currency = currency;
  }

  public BigDecimal getAmount() {
    return amount;
  }
  
  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  public CurrencyCode getCurrency() {
    return currency;
  }

  public void setCurrecy(CurrencyCode currency) {
    this.currency = currency;
  }

  public String toString() {
    return getClass().getName() + ": [ amount=" + amount + ", currency=" + currency.key + "/" + currency.code + " ]";
  }
}