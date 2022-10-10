package com.flink.application.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/***
 * @author M.Tugra Er
 *     10.10.2022
 */
@Getter
@Setter
@ToString
public class IbbWifi implements Serializable {

    private static final long serialVersionUID = 306393547503230762L;

    public IbbWifi(Integer s0, String s1, String s2, String s3, String s4, String s5, String s6, Integer s7) {
        this.id = s0;
        this.date = s1;
        this.subscriberType = s2;
        this.location = s3;
        this.neighborHood = s4;
        this.locationCode = s5;
        this.locationValue = s6;
        this.userCount = s7;

    }
    private Integer id;
    private String date;
    private String subscriberType;
    private String location;
    private String neighborHood;
    private String locationCode;
    private String locationValue;
    private Integer userCount;
    private Integer totalUserCount;
}
