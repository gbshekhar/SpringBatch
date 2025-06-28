package com.springbatch.processor;

import com.springbatch.domain.OSProduct;
import com.springbatch.domain.Product;
import org.springframework.batch.item.ItemProcessor;

public class TransformMyOSProductItemProcessor implements ItemProcessor<Product, OSProduct> {
    @Override
    public OSProduct process(Product item) throws Exception {
        System.out.println("TransformMyOSProductItemProcessor Process executeed");
        OSProduct osProduct = new OSProduct();
        osProduct.setProductId(item.getProductId());
        osProduct.setProductName(item.getProductName());
        osProduct.setProductCategory(item.getProductCategory());
        osProduct.setProductPrice(item.getProductPrice());
        osProduct.setTaxPercent(item.getProductPrice() > 1000 ? 10 : 5);
        osProduct.setSku(item.getProductName() + item.getProductCategory());
        osProduct.setShippingRate(item.getProductPrice() < 1000 ? 75 : 0);
        return osProduct;
    }
}
