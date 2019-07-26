package de.fraunhofer.fokus.ids.persistence.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public abstract class BaseEntity {

    private Long id;
    @JsonProperty("created_at")
    private Date createdAt;
    @JsonProperty("updated_at")
    private Date updatedAt;

    public void createdAt() {
        this.createdAt = this.updatedAt = new Date();
    }

    public void updatedAt() {
        this.updatedAt = new Date();
    }

    @Override
    public boolean equals(Object obj) {
        return obj.getClass() != BaseEntity.class
                && ((BaseEntity)obj).id != null
                && ((BaseEntity)obj).id.equals(this.id);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Date updatedAt) {
        this.updatedAt = updatedAt;
    }

}
