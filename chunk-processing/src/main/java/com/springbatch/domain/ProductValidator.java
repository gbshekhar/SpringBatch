package com.springbatch.domain;

import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;

import java.util.Arrays;
import java.util.List;

public class ProductValidator implements Validator<Product> {

    List<String> validProductCategories = Arrays.asList("Mobile, tablet");

    @Override
    public void validate(Product value) throws ValidationException {
       if(!validProductCategories.contains(value.getProductCategory())){
           throw new ValidationException("Invalid Product Category");
       }
    }
}
