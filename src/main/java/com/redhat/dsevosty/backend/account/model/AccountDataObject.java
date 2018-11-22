package com.redhat.dsevosty.backend.account.model;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

import com.redhat.dsevosty.common.AccountStatusCode;
import com.redhat.dsevosty.common.CurrencyCode;
import com.redhat.dsevosty.common.model.AbstractDataObject;
import com.redhat.dsevosty.common.model.Versionable;

import io.vertx.core.json.JsonObject;

public class AccountDataObject implements AbstractDataObject, Versionable {

    private static final long serialVersionUID = AccountDataObject.class.hashCode();

    private UUID id;
    private String number;
    private String currencyISO4217;
    private boolean credit;
    private BigDecimal amount;
    private String status;
    private UUID metaId;
    private UUID version;

    public AccountDataObject() {
        this.id = defaultId();
        currencyISO4217 = CurrencyCode.RUB.name();
        number = "ААААА-BBB-C-DDDD-EEEEEEE";
        credit = false;
        amount = new BigDecimal("0.00");
        status = AccountStatusCode.CREATED.name();
        metaId = null;
        this.version = null;
    }

    public AccountDataObject(AccountDataObject other) {
        this.id = other.id;
        this.number = other.number;
        this.currencyISO4217 = other.currencyISO4217;
        this.credit = other.credit;
        this.amount = other.amount;
        this.status = AccountStatusCode.CREATED.name();
        this.metaId = other.metaId;
        this.version = null;
    }

    public AccountDataObject(UUID id, String number, String currencyISO4217, boolean credit, BigDecimal amount) {
        this.id = id;
        this.number = number;
        this.currencyISO4217 = currencyISO4217;
        this.credit = credit;
        this.amount = amount;
        this.status = AccountStatusCode.CREATED.name();
        this.metaId = null;
        this.version = null;
    }

    public AccountDataObject(JsonObject json) {
        String val;
        val = json.getString("id");
        if (val != null) {
            id = UUID.fromString(val);
        }
        val = json.getString("number");
        if (val != null) {
            number = val;
        }
        val = json.getString("currencyISO4217");
        if (val != null) {
            currencyISO4217 = val;
        }
        credit = Boolean.getBoolean(val);
        val = json.getString("amount");
        if (val != null) {
            amount = new BigDecimal(val);
        }
        val = json.getString("status");
        if (val != null) {
            status = val;
        }
        val = json.getString("metaId");
        if (val != null) {
            metaId = UUID.fromString(val);
        }
        val = json.getString("version");
        if (val != null) {
            version = UUID.fromString(val);
        }
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

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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
        json.put("number", number);
        json.put("currencyISO4217", currencyISO4217);
        json.put("credit", credit);
        json.put("amount", amount.toString());
        json.put("status", status);
        if (metaId != null) {
            json.put("metaId", metaId.toString());
        }
        if (version != null) {
            json.put("version", versionAsString());
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
        return Objects.equals(getId(), other.getId()) && Objects.equals(getAmount(), other.getAmount())
                && Objects.equals(getCurrencyISO4217(), other.getCurrencyISO4217())
                && Objects.equals(getNumber(), other.getNumber());
    }

    public boolean exactlyEquals(Object o) {
        return this.equals(o) && isVersionEqual(o);
    }

    @Override
    public boolean isVersionEqual(Object other) {
        if (version == null) {
            return false;
        }
        if (other == null) {
            return false;
        }
        if (!(other instanceof Versionable)) {
            return false;
        }
        return versionAsString().equals(((Versionable)other).versionAsString());
    }

    @Override
    public boolean isVersionSet() {
        return version != null;
    }

    @Override
    public synchronized void setVersion() {
        if (version == null) {
            version = defaultId();
        }
    }

    @Override
    public String versionAsString() {
        if (version == null) {
            return "";
        }
        return version.toString();
    }

    // protected synchronized void setVersion(UUID version) {
    //     this.version = version;
    // }
}
