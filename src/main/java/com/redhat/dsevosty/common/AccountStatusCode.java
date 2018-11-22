package com.redhat.dsevosty.common;

public enum AccountStatusCode {
    
    ACTIVE("ACTIVE", "Активный счет"), 
    BLOCKED("BLOCKED", "Счет заблокирован"),
    CREATED("CREATED", "Счет создан"),
    CLOSED("CLOSED", "Счет закрыт"),
    FREEZE("FREEZE", "Счет заморожен");

    public final String code;
    public final String desc;
  
    private AccountStatusCode(String code, String desc) {
      this.code = code;
      this.desc = desc;
    }
  
}
