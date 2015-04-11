package com.happy.hackweb.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;

@Path("/activity")
public class HappyHackJSONService {
	
	
	  @POST
	  @Consumes(MediaType.APPLICATION_JSON)
	  public Response createXmlEventsInJSON(@Context Request request){
		  
		  System.out.println("request is==="+request);
		  
		  return Response.status(200).entity("").build();
		  
		  
	  }
	
	

}
