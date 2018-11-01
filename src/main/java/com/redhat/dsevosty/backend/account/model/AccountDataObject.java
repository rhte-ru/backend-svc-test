package com.redhat.dsevosty.backend.account.model;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

import com.redhat.dsevosty.common.model.AbstractDataObject;

import io.vertx.core.json.JsonObject;

public class AccountDataObject implements AbstractDataObject {

    private static final long serialVersionUID = AccountDataObject.class.hashCode();

    private UUID id;
    private String currencyISO4217;
    private boolean credit;
    private BigDecimal amount;
    private UUID metaId;

    public AccountDataObject() {
        this.id = defaultId();
        currencyISO4217 = "RUB";
        credit = false;
        amount = new BigDecimal(0);
        metaId = null;
    }

    public AccountDataObject(AccountDataObject other) {
        this.id = other.id;
        this.currencyISO4217 = other.currencyISO4217;
        this.credit = other.credit;
        this.amount = other.amount;
        this.metaId = other.metaId;
    }

    public AccountDataObject(UUID id, String currencyISO4217, boolean credit, BigDecimal amount, UUID metaId) {
        this.id = id;
        this.currencyISO4217 = currencyISO4217;
        this.credit = credit;
        this.amount = amount;
        this.metaId = metaId;
    }

    public AccountDataObject(JsonObject json) {
        String val;
        val = json.getString("id");
        if (val != null) {
            id = UUID.fromString(val);
        }
        val = json.getString("currencyISO4217");
        if (val != null) {
            currencyISO4217 = val;
        }
        val = json.getString("credit");
        if (val != null) {
            credit = Boolean.getBoolean(val);
        }
        val = json.getString("amount");
        if (val != null) {
            amount = new BigDecimal(val);
        }
        val = json.getString("metaId");
        if (val != null) {
            metaId = UUID.fromString(val);
        }
    }

    public static UUID defaultId() {
        return UUID.randomUUID();
    }

    @Override
    public UUID getId() {
        if (id == null) {
            id = defaultId();
        }
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getCurrencyISO4217() {
        return currencyISO4217;
    }

    public void setCurrencyISO4217(String currencyISO4217) {
        this.currencyISO4217 = currencyISO4217;
    }

    public boolean isCredit() {
        return credit;
    }

    public void setCredit(boolean credit) {
        this.credit = credit;
    }

    public BigDecimal getAmount() {
        return amount;
    }
    
    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public UUID getMetaId() {
        return metaId;
    }

    public void setMetaId(UUID metaId) {
        this.metaId = metaId;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.put("id", id.toString());
        json.put("currencyISO4217", currencyISO4217);
        json.put("credit", credit);
        json.put("amount", amount.toString());
        if (metaId != null) {
            json.put("metaId", metaId.toString());
        }
        return json;
    }

    public String toString() {
        return toStringAbstract();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof AccountDataObject)) {
            return false;
        }
        AccountDataObject other = (AccountDataObject) o;
        return Objects.equals(getId(), other.getId());
    }
}
