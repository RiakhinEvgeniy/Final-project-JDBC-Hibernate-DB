package com.javarush.dao;

import com.javarush.domain.City;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

import java.util.List;

public class CityDAO {
    private final SessionFactory sessionFactory;

    public CityDAO(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public List<City> getItems(int offset, int count) {
        Query<City> cityQuery = sessionFactory.getCurrentSession().createQuery("select c from City c", City.class);
        cityQuery.setFirstResult(offset);
        cityQuery.setMaxResults(count);
        return cityQuery.list();
    }

    public int getTotalCount() {
        Query<Long> query = sessionFactory.getCurrentSession().createQuery("select count(c) from City c", Long.class);
        return Math.toIntExact(query.uniqueResult());
    }

    public City getById(Integer id) {
        String query = "select c from City c join fetch c.countryId where c.id = :ID";
        Query<City> cityQuery = sessionFactory.getCurrentSession().createQuery(query, City.class);
        cityQuery.setParameter("ID", id);
        return cityQuery.getSingleResult();
    }
}
