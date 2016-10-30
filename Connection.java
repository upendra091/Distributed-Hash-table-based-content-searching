package LAB5;

import java.io.BufferedReader;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Connection {
	String bsIp =null ;
	int bsPort = 0 ;
	String myip = null ;
	Socket bsSocket = null ;
	BufferedReader br = null ;
	PrintWriter pw = null ;
	HashMap < String , String > keyVsIp = new HashMap <String , String > () ;
	ArrayList <SimpleImmutableEntry<BigInteger,String>> nodes = null ;
	HashMap <String , ArrayList<String >> myKeys = null ;
	int myport ;
	Connection(String bsIp , int bsPort , String myip , int myport)
	{
		this.bsIp = bsIp ;
		this.bsPort = bsPort ; 
		this.myip = myip ;
		this.myport = myport ;
		nodes = new ArrayList< SimpleImmutableEntry<BigInteger,String>> () ;
		myKeys = new HashMap < String , ArrayList<String> > () ;
	}
	boolean connectWithBs ()
	{
		try {
			bsSocket = new Socket (bsIp,bsPort);
			bsSocket.setKeepAlive(true);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false ;
		}
		return true ;
	}
	boolean writeToBs (String message )
	{
		try
		{
			if( pw == null )
			{
				if(!connectWithBs())
				{
					return false ;
				}
			}
			message = message+"\r";
			bsSocket.getOutputStream().write(message.getBytes());
			//pw.println(message) ;
			return true ;
		}
		catch (Exception e )
		{
			e.printStackTrace();
			return false;
		}
		
	}
	String readFromBs ()
	{
		try
		{
			byte[] readBuffer = new byte[1024] ;
			bsSocket.getInputStream().read(readBuffer);
			return new String(readBuffer);
		}
		catch (Exception e ){
			e.printStackTrace();
			return null;
		}
	}
	public static String getMsg(String paramString) 
	{ 
		Integer localInteger = Integer.valueOf(paramString.length() + 4);
		String str1 = "0000" + localInteger.toString();
		String str2 = str1.substring(str1.length() - 4, str1.length());
		String str3 = str2 + paramString;     
		return str3;
	}
	boolean getRegistered (String myIp , int myPort )
	{
		String registrationString = " REG "+myIp+" "+myPort+" "+Util.getHash(myIp) ;
		if(writeToBs (getMsg(registrationString)))
		{
			String responce = readFromBs ();
			
			System.out.println( responce ) ;
			if(responce.contains("REGOK") )
			{
				String [] tokens = responce.split(" ");
			//	System.out.println(nodes[2]);
				int numberOfNodes = Integer.parseInt ( tokens [ 2 ].trim());
				if( numberOfNodes == 9999)
				{
					return false ;
				}
				else if (numberOfNodes == 9998)
				{
					return false ;
				}
				else if (numberOfNodes == 9997)
				{
					return false ;
				}
				else
				for (int i = 0 ; i < numberOfNodes ; i++ )
				{
					SimpleImmutableEntry <BigInteger , String > simpleEntry = new SimpleImmutableEntry < BigInteger , String > (new BigInteger(tokens[5+(i*3)].trim(),16),tokens [3+(i*3)]+":"+tokens[4+(i*3)]) ;
					//keyVsIp.put (nodes[5+(i*3)].trim() , nodes [3+(i*3)]+":"+nodes[4+(i*3)]);
					nodes.add(simpleEntry);
					
				}
				return true ;
			}
		}
		return false ;
	}
	void listAllNodes ()
	{
		for(String k:keyVsIp.keySet())
		{
			System.out.println(k+":"+keyVsIp.get(k));
		}
	}
	boolean sendKey (int host , String key , ArrayList <String> values )
	{
		String [] ipndport = nodes.get(host).getValue().split(":");
		try
		{
			//ipndport[0] = "localhost" ;
			Socket socket = new Socket (ipndport[0],Integer.parseInt(ipndport[1]));
			for(int i = 0 ; i < values.size() ; i++ )
			{
				socket.getOutputStream().write(getMsg(" ADD "+values.get(i).split(":")[1]+" "+values.get(i).split(":")[2]+" "+key+" "+values.get(i).split(":")[0]+"\n\r").getBytes());
			}
			socket.close();
			return true ;
		}
		catch (Exception e )
		{
			return false ;
		}
	}
	void receiveCommands (String myIp , final int myPort)
	{
		final ExecutorService clientProcessingPool = Executors.newFixedThreadPool(10);
	
		Thread t = new Thread() {
		    public void run() {
		    	try {
                    ServerSocket serverSocket = new ServerSocket(myPort);
                    System.out.println("Waiting for clients to connect...");
                    while (true) {
                        Socket clientSocket = serverSocket.accept();
                        clientProcessingPool.submit(new ClientTask(clientSocket));
                    }
                } catch (IOException e) {
                    System.err.println("Unable to process client request");
                    e.printStackTrace();
                }
		    }
		};
		t.start();
	}
	void printFingerTable()
	{
		System.out.println("Key , Ip , Port") ; 
		for (int i = 0 ; i < nodes.size() ; i ++ )
		{
			SimpleImmutableEntry<BigInteger, String> node = nodes.get(i) ; 
			System.out.println(node.getKey().toString(16) + " , " + node.getValue().split(":")[0] + " , " + node.getValue().split(":")[1] );
		}
	}
	void printKeyTable ()
	{
		for( Map.Entry<String,ArrayList<String>> val :myKeys.entrySet())
		{
			String key = val.getKey() ;
			for (int i = 0 ; i < val.getValue().size() ; i++ )
			{
				String [] tokens = val.getValue().get(i).split(":");
				System.out.println("Key:"+key+" => IP:"+tokens[1]+" PORT:"+tokens[2] +" Song "+tokens[0] ) ;
			}
		}
	}
	void findKeyFromSuccessor(String myKey , int host , String sourceipport)
	{
		String [] ipndport = nodes.get(host).getValue().split(":");
		try
		{
			//ipndport[0] = "localhost" ;
			Socket socket = new Socket (ipndport[0],Integer.parseInt(ipndport[1]));
			
			socket.getOutputStream().write(getMsg(" SER "+sourceipport+" "+myKey+"\n\r").getBytes());
			
			socket.close();
			
		}
		catch (Exception e )
		{
			
		}
	}
	void searchKey (String key ,String sourceIPPort)
	{
		BigInteger myKey = new BigInteger(Util.getHash(myip),16) ;
		BigInteger songKey = new BigInteger(Util.getHash(key),16) ;
		BigInteger resultKey = myKey ;
		
		int index = 0 ;
		for (int i = 0 ; i < nodes.size() ; i++ )
		{
			if( nodes.get(i).getKey() .compareTo(songKey) < 0 )
			{
				resultKey = nodes.get(i).getKey() ;
				index = i ;
			}
			else
			{
				break;
			}
		}
		if (!resultKey.equals(myKey ))
		{
			findKeyFromSuccessor(key,index,sourceIPPort);
		}
		else
		{
			if(myKeys.containsKey(songKey))
			{
				if(sourceIPPort.equals(myip+" "+myport))
				{
					System.out.println("Search Key is found");
					ArrayList<String> tokens = myKeys.get(songKey) ;
					for (int i = 0 ; i < tokens.size() ; i++)
					{
						System.out.println("File found at "+tokens.get(i));
					}
				}
				else
				{
					String result = "Search Key is found\n";
					ArrayList<String> tokens = myKeys.get(songKey) ;
					for (int i = 0 ; i < tokens.size() ; i++)
					{
						result = result +"File found at "+tokens.get(i)+"\n";
					}
					try
					{
						String [] ipndport = sourceIPPort.split(" ");
					//	ipndport[0] = "localhost" ;
						Socket socket = new Socket (ipndport[0],Integer.parseInt(ipndport[1]));
						
						socket.getOutputStream().write(getMsg(" SER Found "+result+"\n\r").getBytes());
						
						socket.close();
					}
					catch (Exception e )
					{
						
					}
				}
			}
			else
			{
				if(sourceIPPort.equals(myip+" "+myport))
				{
					System.out.println("The Key is not found");
				}
				else
				{
					try
					{
						String [] ipndport = sourceIPPort.split(" ");
					//	ipndport[0] = "localhost" ;
						Socket socket = new Socket (ipndport[0],Integer.parseInt(ipndport[1]));
						
						socket.getOutputStream().write(getMsg(" SER Not Found"+"\n\r").getBytes());
						
						socket.close();
					}
					catch (Exception e )
					{
						
					}
				}
			}
		}
	}
	void updatePeerList()
	{
		
	}
	 private class ClientTask implements Runnable {
	        private final Socket clientSocket;

	        private ClientTask(Socket clientSocket) {
	            this.clientSocket = clientSocket;
	        }
	        void readFromSocket ()
	        {
		        try{
		        	while(clientSocket != null )
		        	{
		        		byte[] readBuffer = new byte[1024] ;
		    			clientSocket.getInputStream().read(readBuffer);
		    			String temp = new String(readBuffer) ;
		    			System.out.println(temp);
		    			String []command =  temp.split(" ") ;
		    			System.out.println(temp.length()+":"+command[0]);
		    			if(command[1].equals("ADD"))
		    			{
		    				
		    				ArrayList<String> al= new ArrayList<String>();
		    				al.add(command[5]+":"+command[2]+":"+command[3]);
		    				HashMap<String,ArrayList<String>> hm = new HashMap<String,ArrayList<String> >();
		    				hm.put(command[4],al);
		    				forwardKeys ( hm , myip);
		    				
		    			}
		    			else if(command[1].equals("SER"))
			    		{
			    				if(command[2].equals("Not"))
			    				{
			    					System.out.println("Key not found") ;
			    				}
			    				else
			    				{
			    					System.out.println(temp);
			    				}
			    		}
		    			
		        	}
		        }
		        catch (Exception e )
		        {
		        	//e.printStackTrace(); 
		        }
	        }
			@Override
			public void run() {
				// TODO Auto-generated method stub
				readFromSocket();
			}
	 }
	
	void forwardKeys (HashMap <String, ArrayList<String> > songs , String myIp)
	{
		Collections.sort(nodes,new Comparator<SimpleImmutableEntry <BigInteger , String >> ()
		{
					

						@Override
						public int compare(
								SimpleImmutableEntry<BigInteger, String> arg0,
								SimpleImmutableEntry<BigInteger, String> arg1) {
							// TODO Auto-generated method stub
							return arg0.getKey().compareTo(arg1.getKey()) ;
						
						}	}
		);
		for (String song : songs.keySet())
		{
			BigInteger myKey = new BigInteger(Util.getHash(myIp),16) ;
			BigInteger songKey = new BigInteger(song,16) ;
			BigInteger resultKey = myKey ;
			
			int index = 0 ;
			for (int i = 0 ; i < nodes.size() ; i++ )
			{
				if( nodes.get(i).getKey() .compareTo(songKey) < 0 )
				{
					resultKey = nodes.get(i).getKey() ;
					index = i ;
				}
				else
				{
					break;
				}
			}
			if (!resultKey.equals(myKey ))
			{
				if(!sendKey (index , song , songs.get(song)))
				{
					System.out.println("Error");
				}
				else
				{
					System.out.println("File Transfered");
				}
			}
			else
			{
				if(!myKeys.containsKey(song))
				{
					myKeys.put(song,new ArrayList <String> ()) ;
				}
				myKeys.get(song).addAll(songs.get(song));
			}
			
		}
	}

	
	boolean getUnregister (String myIp)
	{
		String registrationString = " UNREG "+Util.getHash(myIp) ;
		if(writeToBs (getMsg(registrationString)) )
		{
			String responce = readFromBs ();
			System.out.println( responce ) ;
			if(responce.contains("UNROK") )
			{
				return true ;
			}
		}
		return false ;
	}
	void close()
	{
		try {
			bsSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
}
