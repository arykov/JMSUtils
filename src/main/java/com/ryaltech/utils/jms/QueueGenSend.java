package com.ryaltech.utils.jms;


import java.io.*;
import java.util.*;
import javax.naming.*;
import javax.jms.*;

public class QueueGenSend
{
/**
 * Defines the queue.
 */
  public  static String QUEUE;
  
  
  private QueueConnectionFactory qconFactory;
  private QueueConnection qcon;
  private QueueSession qsession;
  private QueueSender qsender;
  private javax.jms.Queue queue;
  
  

  /**
   * Creates all the necessary objects for sending
   * messages to a JMS queue.
   *
   * @param ctx JNDI initial context
   * @param queueName name of queue
   * @exception NamingException if operation cannot be performed
   * @exception JMSException if JMS fails to initialize due to internal error
   */
  public void init(Context ctx, String queueName, String jmsFactory)
       throws NamingException, JMSException
  {
  	
  	System.out.println("Performing lookups...");
    qconFactory = (QueueConnectionFactory) ctx.lookup(jmsFactory);
    System.out.println("Found queue connection factory...");
    
    qcon = qconFactory.createQueueConnection();
    
    
    qsession = qcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    
    queue = (javax.jms.Queue) ctx.lookup(queueName);
    System.out.println("Found the queue...");
    
    qsender = qsession.createSender(queue);
    
    
    qcon.start();
    
  }

  /**
   * Sends a message to a JMS queue.
   *
   * @params message  message to be sent
   * @exception JMSException if JMS fails to send message due to internal error
   */
  public void send(String message, Properties props)
       throws JMSException
  {
	TextMessage msg = qsession.createTextMessage();
	
    msg.setText(message);
    for (Enumeration e = props.keys() ; e.hasMoreElements() ;) {
         String key = (String)e.nextElement();         
         msg.setStringProperty(key, props.getProperty(key));
     }

    qsender.send(msg);
    System.out.println("Message id "+ msg.getJMSMessageID());
  }

  /**
   * Closes JMS objects.
   * @exception JMSException if JMS fails to close objects due to internal error
   */
  public void close()
       throws JMSException
  {
    qsender.close();
    qsession.close();
    qcon.close();
  }
  
  public static void help(){
  	System.out.println("Usage: java QueueGenSend -jf JNDIFactory -url providerURL -f JMSFactoryJNDIName -q queueJNDIName [-a authenticationType -u username -p password] [-r repeattimes -m message | -file fileName] [ -property name value]");
  	System.out.println("For example for WebLogic: java com.arykov.utils.jms.QueueGenSend -jf weblogic.jndi.WLInitialContextFactory -url t3://localhost:7001 -f jmsCF -q jmsQ");
  	System.out.println("For example for MQ: java com.arykov.utils.jms.QueueGenSend -jf com.sun.jndi.fscontext.RefFSContextFactory -url file:mq -f qcf -q queue");

  }
 /** main() method.
  *
  * @param args WebLogic Server URL
  * @exception Exception if operation fails
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
	String repeatStr = null;
	Properties props = new Properties();
	int repeat = 0;
	String message = null;
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
    	else if(flag.equals("-r"))repeatStr = args[++i];
    	else if(flag.equals("-property")){
    	 
    	 props.setProperty(args[++i], args[++i]);
    	}
    	else if(flag.equals("-m")){
    		if(message != null){
    			System.out.println("Either message specified more than once or both file and message are specified.");
    			help();
    			System.exit(-1);
    		}
    		message = args[++i];
    	}
    	else if(flag.equals("-file")){
    		String fileName = args[++i];
    		if(message != null){
    			System.out.println("Either file was specified more than once or both file and message are specified.");
    			help();
    			System.exit(-1);
    		}
    		BufferedReader br = null;
    		FileReader fr = null;
    		try{
    			fr = new FileReader(fileName);
    			br = new BufferedReader(fr);
    			message = "";
        	while(br.ready()){
        		String readLine = br.readLine();
        	  message += readLine;
        	};
        }catch(Exception ex){
        	System.out.println("Unable to read file: " + fileName);
        	ex.printStackTrace();
        	System.exit(-1);
        }finally{    
        	try{    	
						br.close();
						fr.close();
					}catch(Exception ex){
						//swallow who cares
					}
				}
    		
    	}
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
    if(repeatStr != null){
    	try{
    		repeat = Integer.parseInt(repeatStr);
    	}catch(Exception ex){
    		System.out.println("Repeat should be positive integer.");
    		help();
    		System.exit(-1);
    	}
    	if(repeat <= 0 ){
    		System.out.println("Repeat should be positive integer.");
    		help();
    		System.exit(-1);    		
    	}
    	
    	if(message == null){
    		System.out.println("Repeat cannot be specified without the message.");
    		help();
    		System.exit(-1);    		
    		
    	}
    }
    if(message != null && repeat == 0){
    	repeat = 1;
    }
    try{
	    InitialContext ic = getInitialContext(jndiFactory, providerURL, authenticationType, userID, password);
	    
	    QueueGenSend qs = new QueueGenSend();
	    
	    qs.init(ic, jmsQueue, jmsFactory);
	    if(message == null)readAndSend(qs, props);
	    else repetativeSend(message, repeat, qs, props);
    	qs.close();
    }catch(JMSException ex){
    	ex.printStackTrace();
    	Exception linkedEx = ex.getLinkedException();
    	if(linkedEx != null)linkedEx.printStackTrace();
    	throw ex;
    }
  }

	private static void repetativeSend(String msg, int repeat, QueueGenSend qs, Properties props)
       throws IOException, JMSException
  {
    
    
    for(int i = 0; i < repeat; i++){      
        qs.send(msg, props);
        System.out.println("JMS Message Sent: " + msg + " Repeat: " + (i + 1) + "\n");     
      
    } 

  }

	
  private static void readAndSend(QueueGenSend qs, Properties props)
       throws IOException, JMSException
  {
    BufferedReader msgStream = new BufferedReader(new InputStreamReader(System.in));
    String line=null;
    boolean quitNow = false;
    do {


      System.out.print("Enter message (\"quit\" to quit): ");
      line = msgStream.readLine();
      
      if (line != null && line.trim().length() != 0 && !(quitNow = line.equalsIgnoreCase("quit"))) {
        qs.send(line, props);
        System.out.println("JMS Message Sent: "+line+"\n");
        
        
        
      }
    } while (! quitNow);

  }

  private static InitialContext getInitialContext(String jndiFactory, String url, String authentication, String userID, String userPWD)
       throws NamingException
  {
    Hashtable env = new Hashtable();
    env.put(Context.INITIAL_CONTEXT_FACTORY, jndiFactory);
    env.put(Context.PROVIDER_URL, url);    
    
    if(authentication != null){
            env.put(Context.SECURITY_AUTHENTICATION, authentication);
    }
    if(userID != null){
            env.put(Context.SECURITY_PRINCIPAL, userID);
    }            
    if(userPWD!=null){
            env.put(Context.SECURITY_CREDENTIALS, userPWD); 
    }
    return new InitialContext(env);
  }

}

