package com.ryaltech.utils.jms;

import java.io.*;
import java.util.*;
import javax.transaction.*;
import javax.naming.*;
import javax.jms.*;
import javax.rmi.*;

public class QueueGenReceive
  implements MessageListener
{

  private QueueConnectionFactory qconFactory;
  private QueueConnection qcon;
  private QueueSession qsession;
  private QueueReceiver qreceiver;
  private javax.jms.Queue queue;
  private boolean quit = false;

/**
 * Message listener interface.
 * @param msg  message
 */

  // MessageListener interface
  public void onMessage(Message msg)
  {
    try {
      String msgText;
      if (msg instanceof TextMessage) {
        msgText = ((TextMessage)msg).getText();
      } else {
        msgText = msg.toString();
      }


      System.out.println("Message Received  at " + new java.util.Date().toString()+ ":\n" + msgText );
      System.out.println("Message id " + msg.getJMSMessageID() );
      System.out.println("Correlation id " + msg.getJMSCorrelationID() );
      if (msgText.equalsIgnoreCase("quit")) {
        synchronized(this) {
          quit = true;
          this.notifyAll(); // Notify main thread to quit
        }
      }
    } catch (JMSException ex) {
      ex.printStackTrace();
    	Exception linkedEx = ex.getLinkedException();
    	if(linkedEx != null)linkedEx.printStackTrace();


    }
  }

  /**
   * Creates all the necessary objects for receiving
   * messages from a JMS queue.
   *
   * @param   ctx	JNDI initial context
   * @param	queueName	name of queue
   * @exception NamingException if operation cannot be performed
   * @exception JMSException if JMS fails to initialize due to internal error
   */
  public void init(Context ctx, String jmsQueueJNDI, String jmsFactoryJNDI)
       throws NamingException, JMSException
  {
  	

			qconFactory = (QueueConnectionFactory) PortableRemoteObject.narrow(ctx.lookup(jmsFactoryJNDI), QueueConnectionFactory.class);
    	qcon = qconFactory.createQueueConnection() ;
    	qsession = qcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    	queue = (javax.jms.Queue) PortableRemoteObject.narrow(ctx.lookup(jmsQueueJNDI), javax.jms.Queue.class);
    	qreceiver = qsession.createReceiver(queue);
    	qreceiver.setMessageListener(this);
    	qcon.start();    
  }

  /**
   * Closes JMS objects.
   * @exception JMSException if JMS fails to close objects due to internal error
   */
  public void close()
       throws JMSException
  {
    qreceiver.close();
    qsession.close();
    qcon.close();
  }
/**
  * main() method.
  *
  * @params args  WebLogic Server URL
  * @exception  Exception if execution fails
  */

  public static void main(String[] args)
       throws Exception
  {
  	  	String	 jndiFactory = null;
	String 	 providerURL = null;
	String 	 jmsFactory = null;
	String 	 jmsQueue = null;
	String 	 authenticationType = null;
	String 	 userID = null;
	String 	 password = null;
    int i=0;
    while(i<args.length){
    	String flag = args[i];
    	if(flag.equals("-jf"))jndiFactory = args[++i];
    	else if(flag.equals("-url"))providerURL = args[++i];
    	else if(flag.equals("-f"))jmsFactory = args[++i];
    	else if(flag.equals("-q"))jmsQueue = args[++i];
    	else if(flag.equals("-a"))authenticationType = args[++i];
    	else if(flag.equals("-u"))userID = args[++i];
    	else if(flag.equals("-p"))password = args[++i];
    	else {
    		help();
    		System.exit(-1);
    	}
    	i++;
    }	
    
    if(jndiFactory == null){
    	System.out.println("JNDI factory is not specified");
    	help();
    	System.exit(-1);
    }
    if(providerURL == null){
    	System.out.println("Provider URL is not specified");
    	help();
    	System.exit(-1);
    }
    
    if(providerURL == null){
    	System.out.println("JMS Factory is not specified");
    	help();
    	System.exit(-1);
    }
    if(jmsQueue == null){
    	System.out.println("JMS Queue is not specified");
    	help();
    	System.exit(-1);
    }
    try{
	    InitialContext ic = getInitialContext(jndiFactory, providerURL, authenticationType, userID, password);
	    
	    QueueGenReceive qr = new QueueGenReceive();
	    
	    qr.init(ic, jmsQueue, jmsFactory);
	    
	    System.out.println("JMS Ready To Receive Messages (To quit, send a \"quit\" message).");
	
	    // Wait until a "quit" message has been received.
	    synchronized(qr) {
	      while (! qr.quit) {
	        try {
	          qr.wait();
	        } catch (InterruptedException ie) {}
	      }
	    }
	    qr.close();
	  }catch(JMSException ex){
	  	ex.printStackTrace();
    	Exception linkedEx = ex.getLinkedException();
    	if(linkedEx != null)linkedEx.printStackTrace();
    	System.exit(-1);

	  }
    
  }

  private static InitialContext getInitialContext(String jndiFactory, String url, String authentication, String userID, String userPWD)
       throws NamingException
  {
    Hashtable env = new Hashtable();
    env.put(Context.INITIAL_CONTEXT_FACTORY, jndiFactory);
    env.put(Context.PROVIDER_URL, url);    
    
    if(authentication != null){
            env.put(Context.SECURITY_AUTHENTICATION, authentication);
            env.put(Context.SECURITY_PRINCIPAL, userID);
            env.put(Context.SECURITY_CREDENTIALS, userPWD); 
     }
    return new InitialContext(env);
  }

  public static void help(){
  	System.out.println("Usage: java com.arykov.utils.jms.QueueGenReceive -jf JNDIFactory -url providerURL -f JMSFactoryJNDIName -q queueJNDIName [-a authenticationType -u username -p password]");
  	System.out.println("For example for WebLogic: java com.arykov.utils.jms.QueueGenReceive -jf weblogic.jndi.WLInitialContextFactory -url t3://localhost:7001 -f jmsCF -q jmsQ");
  	System.out.println("For example for MQ: java com.arykov.utils.jms.QueueGenReceive -jf com.sun.jndi.fscontext.RefFSContextFactory -url file:mq -f qcf -q queue");
  }

}




