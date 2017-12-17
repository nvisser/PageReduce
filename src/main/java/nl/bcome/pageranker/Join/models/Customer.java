package nl.bcome.pageranker.Join.models;

import com.google.gson.annotations.SerializedName;

public class Customer {
    @SerializedName("CustomerID")
    int id;
    @SerializedName("CustomerName")
    String name;
    @SerializedName("ContactName")
    String contactName;
    @SerializedName("Country")
    String country;
}
