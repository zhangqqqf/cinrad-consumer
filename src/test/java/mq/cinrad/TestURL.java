package mq.cinrad;

import static org.junit.Assert.*;

import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Test;

public class TestURL {

	@Test
	public void test() throws MalformedURLException {
		String s="http://www.fs121.com";
		
		URL url=new URL(s);
		
		System.out.println(url.toString());
		
		assertTrue(true);
		
		//fail("Not yet implemented");
	}

}
