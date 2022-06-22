package com.example.tableapi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author liuwe 2022/6/22 9:49
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Pojo1 {
    private String id;
    private String name;
    private Long ts;
}
