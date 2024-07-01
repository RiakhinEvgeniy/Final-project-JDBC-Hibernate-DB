package com.javarush;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.javarush.dao.CityDAO;
import com.javarush.dao.CountryDAO;
import com.javarush.domain.City;
import com.javarush.domain.Country;
import com.javarush.domain.CountryLanguage;
import io.lettuce.core.RedisClient;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
        return null;
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
        main.shutdown();
        System.out.println(cities.size());
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
            int totalCount = main.cityDAO.getTotalCount();
            for (int i = 0; i < totalCount; i += STEP) {
                cities.addAll(main.cityDAO.getItems(i, STEP));
            }
            session.getTransaction().commit();
            return cities;
        }
    }
}
