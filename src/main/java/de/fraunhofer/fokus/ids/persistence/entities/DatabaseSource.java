package de.fraunhofer.fokus.ids.persistence.entities;

public class DatabaseSource extends de.fraunhofer.fokus.ids.persistence.entities.DataSource {

    String databaseName;
    String username;
    String password;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
