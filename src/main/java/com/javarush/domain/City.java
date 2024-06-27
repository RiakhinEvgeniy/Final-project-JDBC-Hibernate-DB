package com.javarush.domain;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Entity
@Table(schema = "world", name = "city")
public class City {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;

    @Column(length = 35, name = "name")
    private String name;

    @ManyToOne
    @JoinColumn(name = "country_id")
    private Country countryId;

    @Column(name = "district")
    private String district;

    @Column(name = "population")
    private Integer population;
}
