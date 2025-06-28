package com.springbatch.processor;

import com.springbatch.domain.Product;
import org.springframework.batch.item.ItemProcessor;

public class TransformMyProductItemProcessor implements ItemProcessor<Product, Product> {
    @Override
    public Product process(Product item) throws Exception {
        System.out.println("TransformMyProductItemProcessor Process executeed");
        //in this process we are reducing the price of products by 10%
        Integer price = item.getProductPrice();
        item.setProductPrice((int) (price - (0.1 * price)));
        return item;
    }
}
