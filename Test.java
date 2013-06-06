package com.hb;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"applicationContext.xml");
		Soldier s1 = (Soldier) context.getBean("s1");
		Soldier s2 = (Soldier) context.getBean("s2");
		s2.kill();
		s1.kill();
	}
}
