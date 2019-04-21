package com.sanjiv.repository;

import java.util.List;

import javax.transaction.Transactional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.sanjiv.model.User;

@Transactional
@Repository
public interface UserRepository extends JpaRepository<User, Long>, UserRepositoryCustom {
	
	List<User> findByNameAndEmail(String name, String email);
	
	List<User> findByNameAndEmailIn(String name, List<String> emails);

}

interface UserRepositoryCustom {

	public void someCustomMethod(User user);
}

class UserRepositoryImpl implements UserRepositoryCustom {

	public void someCustomMethod(User user) {
	}
}