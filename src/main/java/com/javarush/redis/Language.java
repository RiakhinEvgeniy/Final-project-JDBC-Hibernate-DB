package com.javarush.redis;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Setter
@Getter
public class Language {

    private String language;
    private Boolean isOfficial;
    private BigDecimal percentage;
}
