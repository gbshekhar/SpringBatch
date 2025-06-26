package com.springbatch.domain;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ProductRowMapper implements RowMapper<Product> {
    @Override
    public Product mapRow(ResultSet rs, int rowNum) throws SQLException {
        Product product = new Product();
        product.setProductId(rs.getInt(1));
        product.setProductName(rs.getString(2));
        product.setProductCategory(rs.getString(3));
        product.setProductPrice(rs.getInt(4));
        return product;
    }
}
