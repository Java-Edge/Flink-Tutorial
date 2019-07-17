package com.javaedge.java.chapter3;

import lombok.Data;
import lombok.ToString;

/**
 * @author JavaEdge
 *
 * @date 2019-07-17
 */
@Data
@ToString
public class Person {
    private String name;
    private int age;
    private String job;
}
