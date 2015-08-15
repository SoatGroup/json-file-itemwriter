package fr.soat.java.spring_batch.jsonitemwriter.model;

import java.io.Serializable;

public class Person implements Serializable {
	
	
	private static final long serialVersionUID = 6615650465830852376L;
	
	private String lastname; 
	private String firstname;
	
	public Person() {}
	
	public Person(String firstname, String lastname){
		this.firstname = firstname; 
		this.lastname = lastname; 
	}


	public String getLastname() {
		return lastname;
	}


	public void setLastname(String lastname) {
		this.lastname = lastname;
	}


	public String getFirstname() {
		return firstname;
	}


	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}


	@Override
	public String toString() {
		return "[lastname=" + lastname + ", firstname=" + firstname
				+ "]";
	}
	
}
