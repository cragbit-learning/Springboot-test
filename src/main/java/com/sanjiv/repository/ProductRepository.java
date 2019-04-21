package com.sanjiv.repository;

import javax.transaction.Transactional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.sanjiv.model.Product;

@Transactional
@Repository
public interface ProductRepository extends JpaRepository<Product, Integer>, ProductRepositoryCustom {
	
}

interface ProductRepositoryCustom {

}

class ProductRepositoryImpl implements ProductRepositoryCustom {

	
}