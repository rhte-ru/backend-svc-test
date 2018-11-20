package com.redhat.dsevosty.backend.account.svc;

// import java.util.UUID;

import com.redhat.dsevosty.backend.account.model.AccountDataObject;
import com.redhat.dsevosty.common.model.AbstractDataObject;
// import com.redhat.dsevosty.common.model.Money;
import com.redhat.dsevosty.common.svc.AbstractDataGridVerticle;

import io.vertx.core.json.JsonObject;

public class AccountDataGridVerticle extends AbstractDataGridVerticle {

  public static final String PACKAGE_NAME = AccountDataGridVerticle.class.getPackage().getName();
  public static final String ARTIFACT_ID = "accout";

  @Override
  protected AbstractDataObject dataObjectFromJson(JsonObject json) {
    return new AccountDataObject(json);
  }

  // public void credit(UUID accountId, Money amount) {
  // }

  @Override
  protected String getPackageName() {
    return PACKAGE_NAME;
  }

  @Override
  protected String getType() {
    return ARTIFACT_ID;
  }
}