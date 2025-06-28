package com.springbatch.processor;

import com.springbatch.domain.Product;
import org.springframework.batch.item.ItemProcessor;

public class FilterProductItemProcessor implements ItemProcessor<Product, Product> {
    @Override
    public Product process(Product item) throws Exception {
        System.out.println("FilterProductItemProcessor executed");
        if(item.getProductPrice() > 1000){
            return item;
        } else {
            return  null;
        }
    }
}
