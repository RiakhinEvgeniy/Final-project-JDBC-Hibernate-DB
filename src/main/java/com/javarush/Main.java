package com.javarush;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.javarush.dao.CityDAO;
import com.javarush.dao.CountryDAO;
import com.javarush.domain.City;
import com.javarush.domain.Country;
import com.javarush.domain.CountryLanguage;
import com.javarush.redis.CityCountry;
import com.javarush.redis.Language;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class Main {

    private final SessionFactory sessionFactory;
    private final ObjectMapper objectMapper;
    private final RedisClient redisClient;
    private final CityDAO cityDAO;
    private final CountryDAO countryDAO;
    private final static int STEP = 500;

    public Main() {
        sessionFactory = prepareRelationalDB();
        cityDAO = new CityDAO(sessionFactory);
        countryDAO = new CountryDAO(sessionFactory);
        objectMapper = new ObjectMapper();
        redisClient = prepareRedisClient();
    }

    private RedisClient prepareRedisClient() {
        RedisClient client = RedisClient.create(RedisURI.create("localhost", 6379));
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            System.out.println("\nConnected to Redis\n"+connection.toString());
        }

        return client;
    }

    private SessionFactory prepareRelationalDB() {
        final SessionFactory sessionFactory;
        Properties properties = new Properties();
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://localhost:3306/world");
        properties.put(Environment.USER, "root");
        properties.put(Environment.PASS, "root");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.HBM2DDL_AUTO, "validate");
        properties.put(Environment.STATEMENT_BATCH_SIZE, "100");

        sessionFactory = new Configuration()
                .addAnnotatedClass(City.class)
                .addAnnotatedClass(Country.class)
                .addAnnotatedClass(CountryLanguage.class)
                .addProperties(properties)
                .buildSessionFactory();
        return sessionFactory;
    }

    public static void main(String[] args) {
        Main main = new Main();
        List<City> cities = main.getAllCities(main);
        List<CityCountry> preparedData = main.transformData(cities);
        main.pushToRedis(preparedData);

        main.sessionFactory.getCurrentSession().close();

        List<Integer> randomCities = List.of(9, 364, 4056, 10, 1024, 2689, 10, 555, 3821);

        long startSQL = System.currentTimeMillis();
        main.testMySQLData(randomCities);
        long endSQL = System.currentTimeMillis();

        long startRedis = System.currentTimeMillis();
        main.testRedisData(randomCities);
        long endRedis = System.currentTimeMillis();

        System.out.printf("%s:\t%d ms\n", "MySQL", endSQL - startSQL);
        System.out.printf("%s:\t%d ms\n", "Redis", endRedis - startRedis);

        main.shutdown();
    }

    private void pushToRedis(List<CityCountry> preparedData) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> commands = connection.sync();
            for (CityCountry cityCountry : preparedData) {
                try {
                    commands.set(String.valueOf(cityCountry.getId()), objectMapper.writeValueAsString(cityCountry));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException();
                }
            }
        }
    }

    private List<CityCountry> transformData(List<City> cities) {
        return cities.stream().map(city -> {
            CityCountry result = new CityCountry();
            result.setId(city.getId());
            result.setName(city.getName());
            result.setPopulation(city.getPopulation());
            result.setDistrict(city.getDistrict());

            Country country = city.getCountryId();
            result.setAlternativeCountryCode(country.getExtraCode());
            result.setContinent(country.getContinent());
            result.setName(country.getName());
            result.setPopulation(country.getPopulation());
            result.setCountryCode(country.getCode());
            result.setCountryRegion(country.getRegion());
            result.setCountrySurfaceArea(country.getSurfaceArea());
            Set<CountryLanguage> countryLanguages = country.getLanguages();
            Set<Language> languages = countryLanguages.stream().map(c1 -> {
                Language language = new Language();
                language.setLanguage(c1.getLanguage());
                language.setIsOfficial(c1.getIsOfficial());
                language.setPercentage(c1.getPercentage());

                return language;

            }).collect(Collectors.toSet());
            result.setLanguages(languages);

            return result;

        }).collect(Collectors.toList());
    }

    private void shutdown() {
        if (nonNull(sessionFactory)) sessionFactory.close();
        if (nonNull(redisClient)) redisClient.shutdown();
    }

    private List<City> getAllCities(Main main) {
        try (Session session = sessionFactory.getCurrentSession()) {
            List<City> cities = new ArrayList<>();
            session.beginTransaction();
            List<Country> countries = main.countryDAO.getAllCountries();
            System.out.println(countries.size());
            int totalCount = main.cityDAO.getTotalCount();
            for (int i = 0; i < totalCount; i += STEP) {
                cities.addAll(main.cityDAO.getItems(i, STEP));
            }
            session.getTransaction().commit();
            return cities;
        }
    }

    private void testRedisData(List<Integer> ids) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();
            for (Integer id : ids) {
                String value = sync.get(String.valueOf(id));
                try {
                    objectMapper.readValue(value, CityCountry.class);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException();
                }
            }
        }
    }

    private void testMySQLData(List<Integer> ids) {
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();
            for (Integer id : ids) {
                City city = cityDAO.getById(id);
                Set<CountryLanguage> languages = city.getCountryId().getLanguages();
                System.out.println(languages.size());
            }
            session.getTransaction().commit();
        }
    }
}
