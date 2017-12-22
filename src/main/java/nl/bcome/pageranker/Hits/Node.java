package nl.bcome.pageranker.Hits;

public class Node {
    String name = "";
    Double auth = 1.0d;
    Double hub = 1.0d;


    public Node() {

    }

    public Node(String name, double auth, double hub) {
        this.name = name;
        this.auth = auth;
        this.hub = hub;
    }

    public String getName() {
        return name;
    }

    public Double getAuth() {
        return auth;
    }

    public Double getHub() {
        return hub;
    }
}