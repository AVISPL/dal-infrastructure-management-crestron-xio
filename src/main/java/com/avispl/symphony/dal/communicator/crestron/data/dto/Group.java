/*
 * Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.crestron.data.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO for crestron XiO Group information
 * <p>
 *
 * @author Maksym.Rossiitsev/Symphony Dev Team<br>
 * Created on April 1, 2026
 */
public class Group {
    private String id;
    @JsonProperty("Name")
    private String name;
    @JsonProperty("ParentGroupId")
    private String parentGroupId;
    @JsonProperty("IsRoom")
    private String isRoom;

    /**
     * Retrieves {@link #id}
     *
     * @return value of {@link #id}
     */
    public String getId() {
        return id;
    }

    /**
     * Sets {@link #id} value
     *
     * @param id new value of {@link #id}
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Retrieves {@link #name}
     *
     * @return value of {@link #name}
     */
    public String getName() {
        return name;
    }

    /**
     * Sets {@link #name} value
     *
     * @param name new value of {@link #name}
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Retrieves {@link #parentGroupId}
     *
     * @return value of {@link #parentGroupId}
     */
    public String getParentGroupId() {
        return parentGroupId;
    }

    /**
     * Sets {@link #parentGroupId} value
     *
     * @param parentGroupId new value of {@link #parentGroupId}
     */
    public void setParentGroupId(String parentGroupId) {
        this.parentGroupId = parentGroupId;
    }

    /**
     * Retrieves {@link #isRoom}
     *
     * @return value of {@link #isRoom}
     */
    public String getIsRoom() {
        return isRoom;
    }

    /**
     * Sets {@link #isRoom} value
     *
     * @param isRoom new value of {@link #isRoom}
     */
    public void setIsRoom(String isRoom) {
        this.isRoom = isRoom;
    }
}
