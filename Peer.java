package LAB5;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

public class Peer {
	String myIpHash = null ;
	String myIp = null ;
    int myPort = 0 ;
    HashMap<String,ArrayList<String>>myFiles = null ;
    Connection connection = null ;
    ArrayList<String> myF = null ;
    Peer (String myIp , int myPort , String bsIp , int bsPort , int n ) throws Exception
    {
    	this.myIp = myIp ;
    	this.myPort = myPort ;
    	myF = new ArrayList <String> (n) ;
    	connection = new Connection (  bsIp , bsPort , myIp , myPort) ;
    	if(connection.connectWithBs())
    	{
    		
    		if(!connection.getRegistered(myIp, myPort))
    		{
    			System.out.println("Registration unsuccessfull");
    			System.exit(0);
    		}
    		connection.receiveCommands(myIp, myPort);
    		
    		}

    	myFiles = new HashMap <String , ArrayList <String> > (n);
    	loadFiles("filenames.txt",n);
    	connection.forwardKeys(myFiles, myIp);
    	connection.updatePeerList();
    }
    void loadFiles(String fileName , int n)
    {
    	try {
			BufferedReader reader = new BufferedReader ( new InputStreamReader ( new FileInputStream(fileName)));
			ArrayList < String > songs = new ArrayList <String > () ;
			String temp = reader.readLine() ;
			while( temp != null )
			{
				songs.add(temp);
				temp = reader.readLine() ;
			}
			for ( int i = 0 ; i < n ; i++ )
			{
				
				int value = Math.abs(new Random().nextInt())%songs.size();
				String songName = songs.get(value);
				songName = songName.replace(' ', '_') ;
				myF.add(songName);
				String [] tokens = songName.split("_");
				for (int j = 0 ; j < tokens.length ; j ++ )
				{
					if(!myFiles.containsKey(Util.getHash(tokens[j])))
					{
						myFiles.put(Util.getHash(tokens[j]), new ArrayList <String > ());
					}
					myFiles.get(Util.getHash(tokens[j])).add(songName+":"+myIp+":"+myPort);
				}
				if(!myFiles.containsKey(Util.getHash(songName)))
				{
					myFiles.put(Util.getHash(songName), new ArrayList <String > ());
				}
				myFiles.get(Util.getHash(songName)).add(songName+":"+myIp+":"+myPort);
				songs.remove(value);
			}
			reader.close();
    	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }
    void printFiles ()
    {
    	System.out.println("My Files ");
    	for (int i = 0 ; i < myF.size() ; i++)
    	{
    		System.out.println(myF.get(i));
    	}
    }
    void processCommand(String command)
	{
		if(command.equals("details"))
		{
			System.out.println("IP:"+myIp+"\nPort:"+myPort+"\nKey:"+Util.getHash(myIp));
		}
		else if (command.equals("fingertable"))
		{
			connection.printFingerTable ();
		}
		else if (command.equals("keytable" ))
		{
			connection.printKeyTable () ;
		}
		else if (command.equals("files"))
		{
			printFiles() ;
		}
		else if (command.equals("search" ))
		{
			System.out.println("Enter the keywords : ");
			Scanner sc = new Scanner (System.in);
			String input = sc.nextLine();
			connection.searchKey (input , myIp+":"+myPort);
		}
		else if (command.equals("exit"))
		{
			connection.getUnregister(myIp);
			try
			{
				connection.bsSocket.close();
			}
			catch ( Exception e )
			{
				e.printStackTrace();
			}
			System.exit(0);
		}
		else if (command.equals("" ))
		{}
		
	}
	//MyPort BootStrapIP BootStapPort
	public static void main ( String args [] )
	{
		try {
			InetAddress IP=InetAddress.getLocalHost();
			Peer peer = new Peer (IP.getHostAddress(),Integer.parseInt(args[0]),args[1],Integer.parseInt(args[2]) , 5) ;
			Scanner sc = new Scanner (System.in) ;
			String temp = sc.nextLine() ;
			while(!temp.equals("exit"))
			{
				peer.processCommand(temp);
				temp = sc.nextLine() ;
			}
		//	peer.connection.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
