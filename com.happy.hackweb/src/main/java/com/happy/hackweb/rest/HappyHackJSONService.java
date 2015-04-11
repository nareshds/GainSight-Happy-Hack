package com.happy.hackweb.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;

import storm.starter.spout.InAppNotificationSpout;
import storm.starter.spout.LineReaderSpout;
import storm.starter.spout.NotificationBolt;
import storm.starter.spout.WordCounterBolt;
import storm.starter.spout.WordSpitterBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

@Path("/activity")
public class HappyHackJSONService {

	private static LocalCluster cluster = new LocalCluster();

	private static void setTopologies() {
		Config config = new Config();
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("inApp-spout", new InAppNotificationSpout());
		builder.setBolt("inApp-bolt", new NotificationBolt()).shuffleGrouping("inApp-spout");
		
		
		cluster.submitTopology("HelloStorm", config, builder.createTopology());
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createXmlEventsInJSON(@Context Request request) {

		System.out.println("request is===" + request);

		return Response.status(200).entity("").build();

	}

	@GET
	public Response shutDown() {

		System.out.println("Stopping the cluster");
		cluster.shutdown();
		return Response.status(200).entity("").build();
	}

}
