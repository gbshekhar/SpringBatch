package com.springbatch.domain;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class ProductFieldSetMapper implements FieldSetMapper<Product> {
    @Override
    public Product mapFieldSet(FieldSet fieldSet) throws BindException {
        Product product = new Product();
        product.setProductId(fieldSet.readInt(0));
        product.setProductName(fieldSet.readString(1));
        product.setProductCategory(fieldSet.readString(2));
        product.setProductPrice(fieldSet.readInt(3));
        return product;
    }
}
