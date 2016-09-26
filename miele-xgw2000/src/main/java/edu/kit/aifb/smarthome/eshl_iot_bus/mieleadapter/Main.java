package edu.kit.aifb.smarthome.eshl_iot_bus.mieleadapter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import edu.kit.aifb.smarthome.eshl_iot_bus.mieleadapter.homebus.MieleApplianceRawData;
import edu.kit.aifb.smarthome.eshl_iot_bus.mieleadapter.homebus.MieleDeviceHomeBusData;
import edu.kit.aifb.smarthome.eshl_iot_bus.mieleadapter.homebus.MieleDeviceList;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;
import ws.wamp.jawampa.ApplicationError;
import ws.wamp.jawampa.WampClient;
import ws.wamp.jawampa.WampClientBuilder;
import ws.wamp.jawampa.connection.IWampConnectorProvider;
import ws.wamp.jawampa.transport.netty.NettyWampClientConnectorProvider;


/**
 * Handling the connection to one miele gateway
 * 
 * @author Kaibin Bao, Ingo Mauser
 *
 */
public class Main {
	
	private static Logger logger = Logger.getLogger("eshl.wamp.miele");

	String homebusUrl;
	
	private HttpClient httpclient;
	private HttpContext httpcontext;

	private ExecutorService executor;
	private Scheduler rxScheduler;
	
	private WampClient client;

	//private Subscription homeBusPolling;
	private Subscription wampPublication;
	private Subscription rpcStartRegistration;
	private Subscription rpcStopRegistration;
	private Subscription rpcLightOnRegistration;
	private Subscription rpcLightOffRegistration;

	private Map<Integer, MieleDeviceHomeBusData> deviceData;
	private BehaviorSubject<Map<Integer, MieleDeviceHomeBusData>> deviceDataObservable;
	
	// error states
	private boolean connectionOK = true;
    private boolean marshallHomebusOK = true;
    private boolean marshallDetailsOK = true;
    private boolean wampPublicationOK = true;

	public static void main(String[] args) throws InterruptedException, JAXBException {
		Main mieleHomeBusAdapter = new Main(
				"192.168.1.20",
				"",
				"");
		
		mieleHomeBusAdapter.registerHomeBusPolling();
		mieleHomeBusAdapter.registerWampPushing();
	}
	
	public Main(
			String gatewayHostAndPort, 
			String username, 
			String password) {
		
		this.executor = Executors.newSingleThreadExecutor();
		this.rxScheduler = Schedulers.from(this.executor);

		// INITIALIZE Miele Homebus Connector
		this.homebusUrl = "http://" + gatewayHostAndPort + "/homebus/?language=en";
		
		CredentialsProvider defaultCredsProvider;
		RequestConfig defaultRequestConfig;
		
		defaultCredsProvider = new BasicCredentialsProvider();
		defaultCredsProvider.setCredentials(
			    new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT), 
			    new UsernamePasswordCredentials(username, password));
		
		defaultRequestConfig = RequestConfig.custom()
				.setTargetPreferredAuthSchemes(Arrays.asList(AuthSchemes.BASIC))
                .setSocketTimeout(1000)
                .setConnectTimeout(1000)
                .setConnectionRequestTimeout(500)
				.build();
		
		this.httpclient = HttpClients.custom()
				.setDefaultCredentialsProvider(defaultCredsProvider)
				.setDefaultRequestConfig(defaultRequestConfig)
				.build();

		// IMMEDIATE DATA
		this.deviceData = new HashMap<Integer, MieleDeviceHomeBusData>();
		this.deviceDataObservable = BehaviorSubject.create(this.deviceData);
		
		// INITIALIZE WAMP Connector
		WampClientBuilder builder = new WampClientBuilder();
        try {
        	IWampConnectorProvider connectorProvider = new NettyWampClientConnectorProvider();
            builder.withConnectorProvider(connectorProvider)
            	.withUri("ws://wamp-router:8080/ws")
            	.withRealm("eshl")
            	.withInfiniteReconnects()
            	.withReconnectInterval(3, TimeUnit.SECONDS);
        	
            client = builder.build();
        } catch (Exception e) {
            logger.error(e);
            System.exit(1);
        }
	}
	
	public boolean sendCommand(String url) {
		HttpGet httpget = new HttpGet(url);
		boolean success = true;
		try {
			HttpResponse response = httpclient.execute(httpget, httpcontext);				
			if( response.getStatusLine().getStatusCode() != 200 ) {
				logger.warn("miele@home bus driver: error sending command " + url);
				success = false;
			}
			EntityUtils.consume(response.getEntity());
		} catch (IOException e1) {
			httpget.abort();
			logger.warn("miele@home bus driver: error sending command " + url, e1);
			success = false;
		}
		return success;
	}
	
	private void registerWampPushing() {
        // Subscribe on the clients status updates
        client.statusChanged()
              .observeOn(rxScheduler)
              .subscribe(
            t1 -> {
                logger.info("Session status changed to " + t1);

                if (t1 instanceof WampClient.ConnectedState) {
                    // PUBLISH
                	wampPublication = deviceDataObservable
                			.observeOn(rxScheduler)
                			.subscribe(new Action1<Map<Integer, MieleDeviceHomeBusData>>() {

						@Override
						public void call(Map<Integer, MieleDeviceHomeBusData> t) {
							if( t.isEmpty() ) { // an error has occurred
            					// ignore?
            					// set state of bus?
							}
							
							// for debugging
							/*
							Map<Integer, MieleDeviceHomeBusData> t_copy = new HashMap<>();
							int i = 0;
							for( int id : t.keySet() ) {
								i++;
								if( i == 3 ) {
									MieleDeviceHomeBusData m = t.get(id);
									ObjectMapper mapper = new ObjectMapper();
									try {
										String json = mapper.writeValueAsString(m);
										logger.warn(json);
									} catch (JsonProcessingException e1) {
										logger.warn(m.toString(), e1);
									}
									continue;
								}
								if( i != 3 )
									t_copy.put(id, t.get(id));
							}
							*/
							
                            // PUBLISH an event
                            client.publish("eshl.miele.v1.homebus", t)
                                  .observeOn(rxScheduler)
                                  .subscribe(
                                t1 -> {
                                	if( !wampPublicationOK ) {
                                		logger.error("publishing to WAMP recovered");
                                	}
                                	wampPublicationOK = true;
                                	logger.info("published to wamp");
                                },
                                e -> {
                                	if( wampPublicationOK ) {
                                		logger.error("publishing to WAMP failed", e);
                                	}
                                	wampPublicationOK = false;
                                });  
						}
					});
                	
                	// REGISTER RPCs
                	registerRPCs();
                }
                else if (t1 instanceof WampClient.DisconnectedState) {
                    if( wampPublication != null ) {
                    	wampPublication.unsubscribe();
                    	wampPublication = null;
                    }
                    if( rpcStartRegistration != null ){
                    	rpcStartRegistration.unsubscribe();
                    	rpcStartRegistration = null;
                    }
                    if( rpcStopRegistration != null ){
                    	rpcStopRegistration.unsubscribe();
                    	rpcStopRegistration = null;
                    }
                    if( rpcLightOnRegistration != null ){
                    	rpcLightOnRegistration.unsubscribe();
                    	rpcLightOnRegistration = null;
                    }
                    if( rpcLightOffRegistration != null ){
                    	rpcLightOffRegistration.unsubscribe();
                    	rpcLightOffRegistration = null;
                    }
                }
            },
			t -> {
	            logger.info("Session ended with error " + t);
	        },
			() -> {
				logger.info("Session ended normally");
			});

        client.open();
	}
	
	private void registerRPCs() {
    	rpcStartRegistration = client.registerProcedure("eshl.miele.v1.homebus.start")
    			.observeOn(rxScheduler)
    			.subscribe(
    					request -> {
    						if( request.arguments() == null || request.arguments().size() != 1 ) {
    				            try {
    				                request.replyError(new ApplicationError(ApplicationError.INVALID_PARAMETER));
    				            } catch (ApplicationError e) { }
    						} else {
    				            int mieleUid = request.arguments().get(0).asInt();
    				            MieleDeviceHomeBusData data = deviceData.get(mieleUid);
    				            if( data == null ) {
    				            	request.reply("device not found");
    				            	return;
    				            }
    				            	
    				            MieleApplianceRawData raw = data.getDeviceDetails();
    				            if( raw == null ) {
    				            	request.reply("device action unavailable");
    				            	return;
    				            }
    				            
    				            URL url = raw.getStartCommandUrl();
    				            if( url == null ) {
    				            	request.reply("device action unavailable");
    				            	return;
    				            }

    				            if( sendCommand(url.toString()) )
		            				request.reply("device started successfully");
		            			else
		            				request.reply("communication error");
    				        }
    					},
    					e -> logger.warn(e));
    	
    	rpcStopRegistration = client.registerProcedure("eshl.miele.v1.homebus.stop")
    			.observeOn(rxScheduler)
    			.subscribe(
    					request -> {
    						if( request.arguments() == null || request.arguments().size() != 1 ) {
    				            try {
    				                request.replyError(new ApplicationError(ApplicationError.INVALID_PARAMETER));
    				            } catch (ApplicationError e) { }
    						} else {
    				            int mieleUid = request.arguments().get(0).asInt();
    				            MieleDeviceHomeBusData data = deviceData.get(mieleUid);
    				            if( data == null ) {
    				            	request.reply("device not found");
    				            	return;
    				            }
    				            	
    				            MieleApplianceRawData raw = data.getDeviceDetails();
    				            if( raw == null ) {
    				            	request.reply("device action unavailable");
    				            	return;
    				            }
    				            
    				            URL url = raw.getStopCommandUrl();
    				            if( url == null ) {
    				            	request.reply("device action unavailable");
    				            	return;
    				            }

    				            if( sendCommand(url.toString()) )
		            				request.reply("device started successfully");
		            			else
		            				request.reply("communication error");
    				        }
    					},
    					e -> logger.warn(e));
    	
    	rpcLightOnRegistration = client.registerProcedure("eshl.miele.v1.homebus.lighton")
    			.observeOn(rxScheduler)
    			.subscribe(
    					request -> {
    						if( request.arguments() == null || request.arguments().size() != 1 ) {
    				            try {
    				                request.replyError(new ApplicationError(ApplicationError.INVALID_PARAMETER));
    				            } catch (ApplicationError e) { }
    						} else {
    				            int mieleUid = request.arguments().get(0).asInt();
    				            MieleDeviceHomeBusData data = deviceData.get(mieleUid);
    				            if( data == null ) {
    				            	request.reply("device not found");
    				            	return;
    				            }
    				            	
    				            MieleApplianceRawData raw = data.getDeviceDetails();
    				            if( raw == null ) {
    				            	request.reply("device action unavailable");
    				            	return;
    				            }
    				            
    				            URL url = raw.getLightOnCommandUrl();
    				            if( url == null ) {
    				            	request.reply("device action unavailable");
    				            	return;
    				            }

    				            if( sendCommand(url.toString()) )
		            				request.reply("device started successfully");
		            			else
		            				request.reply("communication error");
    				        }
    					},
    					e -> logger.warn(e));

    	rpcLightOffRegistration = client.registerProcedure("eshl.miele.v1.homebus.lightoff")
    			.observeOn(rxScheduler)
    			.subscribe(
    					request -> {
    						if( request.arguments() == null || request.arguments().size() != 1 ) {
    				            try {
    				                request.replyError(new ApplicationError(ApplicationError.INVALID_PARAMETER));
    				            } catch (ApplicationError e) { }
    						} else {
    				            int mieleUid = request.arguments().get(0).asInt();
    				            MieleDeviceHomeBusData data = deviceData.get(mieleUid);
    				            if( data == null ) {
    				            	request.reply("device not found");
    				            	return;
    				            }
    				            	
    				            MieleApplianceRawData raw = data.getDeviceDetails();
    				            if( raw == null ) {
    				            	request.reply("device action unavailable");
    				            	return;
    				            }
    				            
    				            URL url = raw.getLightOffCommandUrl();
    				            if( url == null ) {
    				            	request.reply("device action unavailable");
    				            	return;
    				            }

    				            if( sendCommand(url.toString()) )
		            				request.reply("device started successfully");
		            			else
		            				request.reply("communication error");
    				        }
    					},
    					e -> logger.warn(e));
	}
	
	private void registerHomeBusPolling() throws JAXBException {
		final JAXBContext context;

		try {
			context = JAXBContext.newInstance(MieleDeviceList.class);
		} catch (JAXBException e1) {
			logger.error("unable to initialize XML marshaller", e1);
			throw e1;
		}
		
		//homeBusPolling = 
		rxScheduler.createWorker().schedulePeriodically(new Action0() {
			
			@Override
			public void call() {
				MieleDeviceList deviceList = new MieleDeviceList();

				// fetch device list
				try {
					HttpGet httpget = new HttpGet(homebusUrl);
					HttpResponse response = httpclient.execute(httpget, httpcontext);
					HttpEntity entity = response.getEntity();
					if( !connectionOK ) {
						logger.error("connection to miele homebus recovered");
					}
					connectionOK = true;
					if (entity != null) {
					    InputStream instream = entity.getContent();
					    
						// Process the XML
						try {
							Unmarshaller unmarshaller = context.createUnmarshaller();
							deviceList = (MieleDeviceList) unmarshaller.unmarshal(instream);
							if( !marshallHomebusOK ) {
								logger.error("marshalling miele homebus xml recovered");
							}
							marshallHomebusOK = true;
						} catch (JAXBException e) {
							if( marshallHomebusOK )
								logger.error("failed to unmarshall miele homebus xml", e);
							marshallHomebusOK = false;
							deviceList.setDevices(Collections.<MieleDeviceHomeBusData>emptyList()); // set empty list
					    } finally {
					        instream.close();
					        if ( deviceList == null ) {
					        	deviceList = new MieleDeviceList();
					        }
					        if ( deviceList.getDevices() == null ) {
					        	deviceList.setDevices(Collections.<MieleDeviceHomeBusData>emptyList()); // set empty list
					        }
					    }
					}
				} catch (IOException e1) {
					deviceList.setDevices(Collections.<MieleDeviceHomeBusData>emptyList()); // set empty list
					if( connectionOK )
						logger.error("connection to miele homebus failed", e1);
					connectionOK = false;
				}
				
				// fetch device details
				for ( MieleDeviceHomeBusData dev : deviceList.getDevices() ) {
					try {
						HttpGet httpget = new HttpGet(dev.getDetailsUrl());
						HttpResponse response = httpclient.execute(httpget, httpcontext);
						HttpEntity entity = response.getEntity();
						if( !connectionOK ) {
							logger.error("connection to miele homebus recovered");
						}
						connectionOK = true;
						if (entity != null) {
						    InputStream instream = entity.getContent();
						    
							// Process the XML
							try {
								Unmarshaller unmarshaller = context.createUnmarshaller();
								MieleApplianceRawData deviceDetails = (MieleApplianceRawData) unmarshaller.unmarshal(instream);
								dev.setDeviceDetails(deviceDetails);
								if( deviceDetails.getStartCommandUrl() != null )
									dev.addAction("start");
								if( deviceDetails.getStopCommandUrl() != null )
									dev.addAction("stop");
								if( deviceDetails.getLightOffCommandUrl() != null )
									dev.addAction("lightoff");
								if( deviceDetails.getLightOnCommandUrl() != null )
									dev.addAction("lighton");								
								if( !marshallDetailsOK ) {
									logger.error("marshalling miele homebus detail xml recovered");
								}
								marshallDetailsOK = true;
							} catch (JAXBException e) {
								if( marshallDetailsOK )
									logger.error("failed to unmarshall miele homebus detail xml", e);
								marshallDetailsOK = false;
						    } finally {
						        instream.close();
						    }
						}
					} catch (IOException e2) {
						if( connectionOK )
							logger.error("connection to miele homebus failed", e2);
						connectionOK = false;
					}
				}
				
				// store device state
				deviceData.clear();
				for ( MieleDeviceHomeBusData dev : deviceList.getDevices() ) {
					deviceData.put(dev.getUid(), dev);
				}
				deviceDataObservable.onNext(deviceData);
				
				logger.debug("fetching homebus complete");
			}
		}, 1000, 1000, TimeUnit.MILLISECONDS);
	}
}
