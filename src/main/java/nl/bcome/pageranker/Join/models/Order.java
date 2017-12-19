package nl.bcome.pageranker.Join.models;

import com.google.gson.annotations.SerializedName;

public class Order {
    @SerializedName("OrderID")
    int id;
    @SerializedName("CustomerID")
    int customerId;
    @SerializedName("OrderDate")
    String date;
}
