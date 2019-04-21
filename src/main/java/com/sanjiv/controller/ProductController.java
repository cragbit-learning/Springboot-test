package com.sanjiv.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.sanjiv.model.Product;
import com.sanjiv.repository.ProductRepository;

@RestController
@RequestMapping(path = "/products")
public class ProductController {

	@Autowired
	ProductRepository productRepository;

	@RequestMapping(method = RequestMethod.GET)
	@ResponseBody
	public List<Product> getAllProduct() {
		List<Product> products = productRepository.findAll();
		if (products.size() == 0) {
			throw new NullPointerException("Product not found.!"); 
		}
		return products;
	}

	@RequestMapping(path = "/{id}", method = RequestMethod.GET)
	@ResponseBody
	public Product getProduct(@PathVariable("id") Integer id) {
		Product product = productRepository.findById(id).orElse(null);
		if (product == null) {
			throw new NullPointerException("Product not found.!"); 
		}
		return product;
	}

	@RequestMapping(path = "/{id}", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public Product updateProduct(@PathVariable("id") Integer id, @RequestBody Product payload) {
		Product product = productRepository.findById(id).orElse(null);
		if (product == null) {
			throw new NullPointerException("Product not found.!"); 
		}
		product.setName(payload.getName());
		product.setDesc(payload.getDesc());
		product.setType(payload.getType());
		product = productRepository.save(product);
		return product;
	}

	@RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public Product addProduct(@RequestBody Product payload) {
		Product product = new Product();
		product.setName(payload.getName());
		product.setDesc(payload.getDesc());
		product.setType(payload.getType());
		product = productRepository.save(product);
		return product;
	}

	@RequestMapping(path = "/{id}", method = RequestMethod.DELETE)
	public Product deleteProduct(@PathVariable("id") Integer id) {
		Product product = productRepository.findById(id).orElse(null);
		if (product == null) {
			throw new NullPointerException("Product not found.!"); 
		}
		productRepository.delete(product);
		return product;
	}

}
