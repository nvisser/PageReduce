package nl.bcome.pageranker.Join.models;

import com.google.gson.annotations.SerializedName;

public class Customer {
    @SerializedName("CustomerID")
    int id;
    @SerializedName("CustomerName")
    String name;
    @SerializedName("ContactName")
    String contactName;

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getContactName() {
        return contactName;
    }

    public String getCountry() {
        return country;
    }

    @SerializedName("Country")
    String country;
}
